"""Monitor Curious alerts and send them to REDCap."""

import argparse
import asyncio
from contextlib import asynccontextmanager
import json
import logging
from typing import Any, AsyncIterator, cast, Optional

from numpy import intersect1d
import pandas as pd
import requests
import websockets
from websockets.exceptions import (
    ConnectionClosedError,
    ConnectionClosedOK,
    InvalidStatus,
)

from .._config_variables import curious_variables, redcap_variables
from ..from_redcap.config import FieldList
from ..from_redcap.from_redcap import fetch_data, response_index_reverse_lookup
from ..utility_functions import (
    CuriousAlert,
    initialize_logging,
    redcap_api_push,
    setup_tsv_logger,
)
from .config import curious_authenticate
from .utils import (
    fetch_alerts_metadata,
    REDCAP_TOKEN,
)

initialize_logging()
logger = logging.getLogger(__name__)
REDCAP_ENDPOINTS = redcap_variables.Endpoints()

# ============================================================================
# Constants
# ============================================================================
ALERT_FIELD_PATTERN = r"alerts_([^_]+(?:_[^_]+)?)_\d+"
PID_625 = REDCAP_TOKEN  # Use token from alert_utils

# WebSocket configuration constants
WS_RECONNECT_DELAY = 5  # seconds
WS_MAX_RECONNECT_ATTEMPTS = None  # None = infinite retries
WS_PING_INTERVAL = 20  # seconds
WS_PING_TIMEOUT = 10  # seconds
WS_CLOSE_TIMEOUT = 5  # seconds

# ============================================================================
# Type Definitions
# ============================================================================


class _SynchronousArgs(argparse.Namespace):
    """Typehints for CLI args."""

    synchronous: bool
    partial: bool
    max_reconnect_attempts: Optional[int]


# ============================================================================
# Authentication & Connection
# ============================================================================


@asynccontextmanager
async def connect_to_websocket(
    token: str, uri: str
) -> AsyncIterator[websockets.ClientConnection]:
    """Connect to a websocket with an auth token and proper configuration."""
    websocket = await websockets.connect(
        uri,
        subprotocols=[cast(websockets.typing.Subprotocol, f"bearer|{token}")],
        ping_interval=WS_PING_INTERVAL,
        ping_timeout=WS_PING_TIMEOUT,
        close_timeout=WS_CLOSE_TIMEOUT,
    )
    try:
        yield websocket
    finally:
        await websocket.close()


# ============================================================================
# Alert Parsing
# ============================================================================


def _parse_alert_message(message: str) -> tuple[str, str]:
    """
    Parse alert message to extract answer and item.

    Returns
    -------
    tuple[str, str]
        (answer, item_name) tuple

    """
    _color, message_remainder = message.split(': "', 1)
    answer, message_remainder = message_remainder.split('"', 1)
    message_remainder, item = message_remainder.rsplit(" ", 1)
    return answer, f"alerts_{item.lower()}"


def parse_alert(alert: CuriousAlert) -> pd.DataFrame:
    """
    Parse an alert from Curious into REDCap format.

    Note: 'record', 'value', and 'redcap_event_name' columns need further processing.
    """
    columns = ["record", "field_name", "value", "redcap_event_name"]
    # Check for secretId FIRST
    if "secretId" not in alert:
        logger.info('Response: \n"""\n%s\n"""\ndoes not include "secretId"', alert)
        tsv_logger = setup_tsv_logger("mrn_error_log", "mrn_error_log.tsv")
        tsv_logger.error(str(alert), extra={"mrn": "", "attempt": "parse_alert"})
        return pd.DataFrame(columns=columns)
    answer, item = _parse_alert_message(alert["message"])
    fields: list[tuple[str, Any]] = [("mrn", alert["secretId"]), (item, answer)]
    data: list[tuple[str, str, Any, Optional[str]]] = [
        (alert["secretId"], field_name, field_value, None)
        for field_name, field_value in fields
    ]
    return pd.DataFrame(data, columns=columns)


# ============================================================================
# REDCap Data Processing
# ============================================================================


def _create_choice_lookup(
    alerts_instrument: pd.DataFrame,
) -> dict[tuple[str, str], int | str]:
    """Create lookup dictionary for mapping response values to REDCap indices."""
    choice_lookup_tuples = [
        lookup_tuple
        for lookup_tuple in [
            item
            for _, row in alerts_instrument.iterrows()
            for item in response_index_reverse_lookup(row)
        ]
        if lookup_tuple
    ]
    return {lookup_tuple[0:2]: lookup_tuple[2] for lookup_tuple in choice_lookup_tuples}


def _map_mrns_to_records(
    redcap_alerts: pd.DataFrame,
    redcap_fields: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, int]]:
    """
    Map MRNs to record IDs and prepare lookups.

    Returns
    -------
    tuple
        (processed_alerts, mrn_lookup)
        - processed_alerts: Filtered alert DataFrame with event names populated
        - mrn_lookup: Maps MRN string to record ID integer

    """
    # Prepare data types
    redcap_alerts["record"] = (
        redcap_alerts["record"].str.replace(r"\D", "", regex=True).astype(int)
    )
    redcap_fields["record"] = redcap_fields["record"].astype(int)
    # Create lookups
    mrn_lookup = cast(
        dict[str, int],
        redcap_fields[redcap_fields["field_name"] == "mrn"]
        .set_index("value")["record"]
        .to_dict(),
    )
    record_events = cast(
        dict[str, str],
        redcap_fields.groupby("field_name")["redcap_event_name"].first().to_dict(),
    )
    # Filter results
    result = redcap_alerts.loc[redcap_alerts["field_name"] != "mrn"].copy()
    result = result[result["field_name"].isin(redcap_fields["field_name"])]
    # Map event names by field name
    result["redcap_event_name"] = result["field_name"].map(record_events)
    return result, mrn_lookup


def toggle_alerts(result: pd.DataFrame) -> pd.DataFrame:
    """Add an `{instrument}_alerts` row for each relevant respondent + instrument."""
    respondent_instruments = result["field_name"].str.extract(
        ALERT_FIELD_PATTERN, expand=False
    )
    summary = result[respondent_instruments.notna()].copy()
    summary["field_name"] = (
        respondent_instruments[respondent_instruments.notna()] + "_alerts"
    )
    summary = summary.drop_duplicates(["record", "field_name", "redcap_event_name"])
    summary["value"] = "yes"
    return pd.concat([result, summary], ignore_index=True)


def process_alerts_for_redcap(
    redcap_alerts: pd.DataFrame, partial_redcap_landing: bool = False
) -> pd.DataFrame:
    """
    Process alerts and prepare them for REDCap push.

    This function:
    1. Fetches relevant REDCap metadata and data
    2. Maps MRNs to record IDs
    3. Converts response values to REDCap indices
    4. Toggles alert flags
    """
    alert_fields = redcap_alerts["field_name"].unique()
    # Fetch metadata
    alerts_instrument = fetch_alerts_metadata(REDCAP_ENDPOINTS.base_url)
    # Filter fields if partial landing
    if partial_redcap_landing:
        alert_fields = intersect1d(
            alert_fields, alerts_instrument["field_name"].unique()
        )
    # Fetch existing REDCap data
    redcap_fields = fetch_data(PID_625, str(FieldList(alert_fields)), all_or_any="any")
    # Map MRNs to records
    result, mrn_lookup = _map_mrns_to_records(redcap_alerts, redcap_fields)
    # Map response values to indices
    choice_lookup = _create_choice_lookup(alerts_instrument)
    result["lookup_key"] = list(
        zip(result["field_name"], result["value"].str.strip().str.lower())
    )
    result["value"] = result["lookup_key"].map(choice_lookup).fillna(result["value"])
    # Toggle alerts and set final record IDs
    result = toggle_alerts(result.drop("lookup_key", axis=1))
    result["record"] = result["record"].map(mrn_lookup)
    return result


def push_alerts_to_redcap(result: pd.DataFrame) -> None:
    """Push processed alerts to REDCap."""
    try:
        redcap_api_push(
            result,
            PID_625,
            REDCAP_ENDPOINTS.base_url,
            redcap_variables.headers,
        )
        logger.info(
            "%d rows successfully updated for alerts in PID 625.", result.shape[0]
        )
    except Exception:
        logger.exception("Pushing alerts from Curious to REDCap failed.")
        raise


# ============================================================================
# Alert Processing Pipeline
# ============================================================================


def _process_single_alert(
    alert: CuriousAlert, partial_redcap_landing: bool = False
) -> Optional[pd.DataFrame]:
    """
    Process a single alert and return result DataFrame or None if invalid.

    Returns
    -------
    Optional[pd.DataFrame]
        Processed alert data ready for REDCap push, or None if no valid data

    """
    # Validate message type
    if alert.get("type") != "answer":
        logger.debug("Skipping non-answer message type: %s", alert.get("type"))
        return None
    # Parse and process
    redcap_alert = parse_alert(alert)
    if redcap_alert.empty:
        result = redcap_alert
    else:
        result = process_alerts_for_redcap(redcap_alert, partial_redcap_landing)
    if result.empty:
        logger.warning("No valid data to push for alert ID: %s", alert.get("id"))
        return None
    return result


def _handle_alert_errors(message: str, error: Exception) -> None:
    """Centralized error handling for alert processing."""
    if isinstance(error, json.JSONDecodeError):
        logger.exception("Failed to parse message as JSON: %s", message)
    elif isinstance(error, KeyError):
        logger.exception("Missing expected field in alert message")
    else:
        logger.exception("Error processing alert message: %s", message)


# ============================================================================
# WebSocket Listener
# ============================================================================


async def websocket_listener(
    websocket: websockets.ClientConnection, partial_redcap_landing: bool = False
) -> None:
    """Listen to websocket messages and process alerts."""
    try:
        async for message in websocket:
            logger.info("Received alert: %s", message)
            try:
                alert: CuriousAlert = json.loads(message)
                result = _process_single_alert(alert, partial_redcap_landing)
                if result is not None:
                    push_alerts_to_redcap(result)
            except Exception as e:
                _handle_alert_errors(str(message), e)
    except ConnectionClosedError:
        logger.warning("WebSocket connection closed")
        raise
    except ConnectionClosedOK:
        logger.info("WebSocket connection closed normally")
    except Exception:
        logger.exception("Unexpected error in websocket listener")
        raise


async def main_with_reconnect(
    token: str,
    uri: str,
    partial_redcap_landing: bool = False,
    max_attempts: Optional[int] = WS_MAX_RECONNECT_ATTEMPTS,
) -> None:
    """
    Automatically reconnect when websocket connection breaks.

    Parameters
    ----------
    token
        Authentication token for WebSocket connection
    uri
        WebSocket URI to connect to
    partial_redcap_landing
        Whether to use partial REDCap landing
    max_attempts
        Maximum number of reconnection attempts. None = infinite.

    """
    attempt = 0
    while max_attempts is None or attempt < max_attempts:
        try:
            if attempt > 0:
                logger.info(
                    "Reconnection attempt %d%s",
                    attempt,
                    f" of {max_attempts}" if max_attempts else "",
                )
            async with connect_to_websocket(token, uri) as websocket:
                # Reset attempt counter on successful connection
                if attempt > 0:
                    logger.info("Successfully reconnected to WebSocket")
                attempt = 0
                await websocket_listener(websocket, partial_redcap_landing)
                logger.info("WebSocket listener completed normally")
                break
        except ConnectionClosedError:
            attempt += 1
            if max_attempts and attempt >= max_attempts:
                logger.exception("Max reconnection attempts reached. Exiting.")
                raise
            logger.warning(
                "Connection lost. Reconnecting in %d seconds...", WS_RECONNECT_DELAY
            )
            await asyncio.sleep(WS_RECONNECT_DELAY)
        except InvalidStatus as e:
            # Authentication or server errors
            logger.exception(
                "WebSocket connection failed with status %s", e.response.status_code
            )
            if e.response.status_code == requests.codes["unauthorized"]:
                logger.exception(
                    "Authentication failed. Token may be invalid or expired."
                )
            attempt += 1
            if max_attempts and attempt >= max_attempts:
                logger.exception("Max reconnection attempts reached. Exiting.")
                raise
            await asyncio.sleep(WS_RECONNECT_DELAY)
        except asyncio.CancelledError:
            logger.info("Operation cancelled")
            raise
        except KeyboardInterrupt:
            logger.info("WebSocket listener cancelled manually")
            break
        except Exception:
            logger.exception("Fatal error in main loop")
            raise


# ============================================================================
# Main Functions
# ============================================================================


async def main(
    partial_redcap_landing: bool = False, max_attempts: Optional[int] = None
) -> None:
    """
    Send Curious alerts to REDCap (async version via websocket).

    This version includes automatic reconnection on connection failures.

    Parameters
    ----------
    partial_redcap_landing
        Whether to use partial REDCap landing
    max_attempts
        Maximum number of reconnection attempts. None = infinite.

    """
    tokens = curious_authenticate()
    endpoints = curious_variables.Endpoints(protocol="wss")
    await main_with_reconnect(
        token=tokens.access,
        uri=endpoints.alerts,
        partial_redcap_landing=partial_redcap_landing,
        max_attempts=max_attempts,
    )


def synchronous_main(partial_redcap_landing: bool = False) -> None:
    """Send Curious alerts to REDCap (synchronous version via REST API)."""
    tokens = curious_authenticate()
    response = requests.get(
        tokens.endpoints.alerts,
        headers=curious_variables.headers(tokens.access),
    )
    if response.status_code != requests.codes["okay"]:
        response.raise_for_status()
        return
    results: list[CuriousAlert] = response.json()["result"]
    # Parse and concatenate all alerts
    redcap_alerts = pd.concat([parse_alert(alert) for alert in results])
    # Process and push to REDCap
    result = process_alerts_for_redcap(redcap_alerts, partial_redcap_landing)
    push_alerts_to_redcap(result)


# ============================================================================
# CLI
# ============================================================================


def cli() -> None:
    """Run asynchronous or synchronous main function."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--asynchronous", action="store_false", dest="synchronous")
    parser.add_argument("--partial", action="store_true", dest="partial")
    parser.add_argument("--synchronous", action="store_true", dest="synchronous")
    parser.add_argument(
        "--max-reconnect-attempts",
        type=int,
        default=None,
        help="Maximum number of reconnection attempts (default: infinite)",
    )
    parser.set_defaults(partial=False, synchronous=False)
    namespace = _SynchronousArgs()
    args = parser.parse_args(namespace=namespace)
    if args.synchronous:
        synchronous_main(args.partial)
    else:
        try:
            asyncio.run(main(args.partial, args.max_reconnect_attempts))
        except KeyboardInterrupt:
            logger.info("Asynchronous connection cancelled manually.")


if __name__ == "__main__":
    cli()
