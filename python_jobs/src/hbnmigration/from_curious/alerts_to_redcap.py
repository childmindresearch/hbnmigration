# alerts_to_redcap.py

"""Monitor Curious alerts and send them to REDCap."""

import argparse
import asyncio
from contextlib import asynccontextmanager
import json
import logging
import re
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
from ..from_redcap.from_redcap import fetch_data
from ..utility_functions import (
    CuriousAlert,
    CuriousAlertHttps,
    CuriousAlertWebsocket,
    DataCache,
    initialize_logging,
    redcap_api_push,
)
from .config import curious_authenticate
from .utils import (
    alert_websocket_to_https,
    call_curious_api,
    create_choice_lookup,
    deduplicate_dataframe,
    fetch_alerts_metadata,
    map_mrns_to_records,
)

initialize_logging()
logger = logging.getLogger(__name__)

REDCAP_ENDPOINTS = redcap_variables.Endpoints()

# ============================================================================
# Constants
# ============================================================================

ALERT_FIELD_PATTERN = r"alerts_([^_]+(?:_[^_]+)?)_\d+"
PID_625 = redcap_variables.Tokens().pid625

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


def _prepare_applet_names(applet_names: Optional[list[str]]) -> list[str]:
    """Give list of applet names from commandline or fallback on all applets."""
    return list(curious_variables.applets.keys()) if not applet_names else applet_names


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
    answer, message_remainder = message_remainder.split('" to', 1)
    message_remainder, item = message_remainder.rsplit(" ", 1)
    return answer, f"alerts_{item.lower()}"


def parse_alert(alert: CuriousAlert) -> pd.DataFrame:
    """
    Parse an alert from Curious into REDCap format.

    Note: 'record', 'value', and 'redcap_event_name' columns need further processing.
    """
    columns = ["record", "field_name", "value", "redcap_event_name"]
    alert = alert_websocket_to_https(alert)
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


def toggle_alerts(result: pd.DataFrame) -> pd.DataFrame:
    """Add an `{instrument}_alerts` row for each relevant respondent + instrument."""
    if result.empty:
        return result
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


def derive_instrument_name(result: pd.DataFrame) -> str:
    """
    Derive instrument name from alert field columns.

    Looks for fields matching patterns like:
    - alerts_{instrument}_{number}
    - {instrument}_alerts

    Parameters
    ----------
    result : pd.DataFrame
        DataFrame containing alert field columns

    Returns
    -------
    str
        Instrument name (e.g., "ra_alerts_parent", "ra_alerts_child")
        Defaults to "ra_alerts_parent" if no match found

    """

    def _normalize_instrument_name(instrument_part: str) -> str:
        """Convert instrument part to standard naming convention."""
        if "parent" in instrument_part or instrument_part.endswith("_p"):
            return "ra_alerts_parent"
        if "child" in instrument_part or instrument_part.endswith("_c"):
            return "ra_alerts_child"
        if instrument_part.startswith("ra_alerts"):
            return instrument_part
        return f"ra_alerts_{instrument_part}"

    # Strategy 1: Check field_name column for alerts_instrument_number pattern
    alert_field_pattern = r"alerts_([a-z_]+)_\d+"
    for col in result.get("field_name", []):
        if pd.notna(col):
            match = re.search(alert_field_pattern, str(col))
            if match:
                return _normalize_instrument_name(match.group(1))

    # Strategy 2: Check field_name column for instrument_alerts pattern
    alerts_suffix_pattern = r"^([a-z_]+)_alerts$"
    for col in result.get("field_name", []):
        if pd.notna(col):
            match = re.search(alerts_suffix_pattern, str(col))
            if match:
                return _normalize_instrument_name(match.group(1))

    # Strategy 3: Check DataFrame column names as fallback
    for col in result.columns:
        if col.endswith("_alerts"):
            instrument_part = col.replace("_alerts", "")
            return _normalize_instrument_name(instrument_part)
        if col.startswith("alerts_"):
            parts = col.split("_")
            if len(parts) >= 2:  # noqa: PLR2004
                return _normalize_instrument_name(parts[1])

    # Default fallback
    default_fallback = "ra_alerts_parent"
    logger.warning(
        "Could not derive instrument name from columns, defaulting to '%s'",
        default_fallback,
    )
    return default_fallback


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
    result, mrn_lookup = map_mrns_to_records(redcap_alerts, redcap_fields)

    # Map response values to indices using generalized function
    choice_lookup = create_choice_lookup(alerts_instrument)
    result["lookup_key"] = list(
        zip(result["field_name"], result["value"].str.strip().str.lower())
    )
    result["value"] = result["lookup_key"].map(choice_lookup).fillna(result["value"])

    # Map record IDs BEFORE toggle_alerts
    result["record"] = result["record"].map(mrn_lookup)

    # Toggle alerts (now working with record IDs, not MRNs)
    return toggle_alerts(result.drop("lookup_key", axis=1))


def push_alerts_to_redcap(result: pd.DataFrame) -> None:
    """Push processed alerts to REDCap with deduplication."""
    if result.empty:
        logger.info("No alerts to push (empty DataFrame)")
        return

    # Derive instrument name from the alert data
    instrument_name = derive_instrument_name(result)
    logger.info("Derived instrument name: %s", instrument_name)

    # Deduplicate before pushing
    result, num_duplicates = deduplicate_dataframe(
        result,
        PID_625,
        REDCAP_ENDPOINTS.base_url,
        redcap_variables.headers,
        instrument_name,
    )

    if result.empty:
        logger.info("All alert rows are duplicates, skipping upload")
        return

    if num_duplicates > 0:
        logger.info("Removed %d duplicate alert rows before push", num_duplicates)

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
    alert: CuriousAlert,
    partial_redcap_landing: bool = False,
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
    websocket: websockets.ClientConnection,
    partial_redcap_landing: bool = False,
) -> None:
    """Listen to websocket messages and process alerts."""
    try:
        async for message in websocket:
            logger.info("Received alert: %s", message)
            try:
                alert: CuriousAlertWebsocket = json.loads(message)
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
    applet_name: str,
    uri: str,
    partial_redcap_landing: bool = False,
    max_attempts: Optional[int] = WS_MAX_RECONNECT_ATTEMPTS,
) -> None:
    """
    Automatically reconnect when websocket connection breaks.

    Parameters
    ----------
    applet_name
        Name of applet to authenticate to
    uri
        WebSocket URI to connect to
    partial_redcap_landing
        Whether to use partial REDCap landing
    max_attempts
        Maximum number of reconnection attempts. None = infinite.

    """
    attempt = 0
    tokens = curious_authenticate(applet_name)

    while max_attempts is None or attempt < max_attempts:
        try:
            if attempt > 0:
                logger.info(
                    "Reconnection attempt %d%s",
                    attempt,
                    f" of {max_attempts}" if max_attempts else "",
                )
            async with connect_to_websocket(tokens.access, uri) as websocket:
                if attempt > 0:
                    logger.info("Successfully reconnected to WebSocket")
                attempt = 0
                await websocket_listener(websocket, partial_redcap_landing)
                logger.info("WebSocket listener completed normally")
                break

        except ConnectionClosedError:
            attempt += 1
            if max_attempts is not None and attempt >= max_attempts:
                logger.exception("Max reconnection attempts reached. Exiting.")
                raise
            logger.warning(
                "Connection lost. Reconnecting in %d seconds...", WS_RECONNECT_DELAY
            )
            await asyncio.sleep(WS_RECONNECT_DELAY)

        except InvalidStatus as e:
            status = e.response.status_code
            logger.exception("WebSocket connection failed with status %s", status)

            if status == requests.codes["unauthorized"]:
                # ── Re-authenticate ──
                logger.warning("Token expired or invalid. Re-authenticating...")
                try:
                    tokens = curious_authenticate(applet_name)
                    logger.info("Re-authentication successful")
                except Exception:
                    logger.exception("Re-authentication failed. Exiting.")
                    raise

            attempt += 1
            if max_attempts is not None and attempt >= max_attempts:
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
    applet_names: list[str],
    partial_redcap_landing: bool = False,
    max_attempts: Optional[int] = None,
) -> None:
    """
    Send Curious alerts to REDCap (async version via websocket).

    This version includes automatic reconnection on connection failures.

    Parameters
    ----------
    applet_names
        List of applet names for which to process alerts
    partial_redcap_landing
        Whether to use partial REDCap landing
    max_attempts
        Maximum number of reconnection attempts. None = infinite.

    """
    for applet_name in applet_names:
        endpoints = curious_variables.Endpoints(protocol="wss")
        await main_with_reconnect(
            applet_name=applet_name,
            uri=endpoints.alerts,
            partial_redcap_landing=partial_redcap_landing,
            max_attempts=max_attempts,
        )


def synchronous_main(
    applet_names: list[str], partial_redcap_landing: bool = False
) -> None:
    """Send Curious alerts to REDCap (synchronous version via REST API)."""
    # Initialize cache for minute-by-minute transfers (TTL: 2 minutes)
    cache = DataCache("curious_alerts_to_redcap", ttl_minutes=2)
    for applet_name in applet_names:
        tokens = curious_authenticate(applet_name)
        results = call_curious_api(
            tokens.endpoints.alerts, tokens, return_type=list[CuriousAlertHttps]
        )

        # Filter out alerts already processed
        unprocessed_alerts: list[CuriousAlertHttps] = []
        for alert in results:
            alert_id = alert.get("id", "")
            # if not cache.is_processed(alert_id):
            unprocessed_alerts.append(alert)
            # else:
            #     logger.debug("Skipping already-processed alert: %s", alert_id)

        if not unprocessed_alerts:
            logger.info("All alerts already processed in cache.")
            return

        logger.info(
            "Processing %d new alerts (skipped %d cached)",
            len(unprocessed_alerts),
            len(results) - len(unprocessed_alerts),
        )

        # Parse and concatenate all new alerts
        redcap_alerts = pd.concat([parse_alert(alert) for alert in unprocessed_alerts])

        # Process and push to REDCap
        # (deduplication happens inside push_alerts_to_redcap)
        result = process_alerts_for_redcap(redcap_alerts, partial_redcap_landing)
        push_alerts_to_redcap(result)

        # Mark alerts as processed in cache
        for alert in unprocessed_alerts:
            alert_id = alert.get("id", "")
            cache.mark_processed(alert_id, metadata={"processed": True})

        # Log cache statistics
        cache_stats = cache.get_stats()
        logger.info(
            "Cache statistics: %d entries, file size: %d bytes, last activity: %s",
            cache_stats["total_entries"],
            cache_stats["file_size_bytes"],
            cache_stats.get("last_activity", "never"),
        )


# ============================================================================
# CLI
# ============================================================================


def cli() -> None:
    """Run asynchronous or synchronous main function."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--asynchronous", action="store_false", dest="synchronous")
    parser.add_argument("--partial", action="store_true", dest="partial")
    parser.add_argument("--synchronous", action="store_true", dest="synchronous")
    parser.add_argument("--applet", type=str, nargs="*", dest="applet_names")
    parser.add_argument(
        "--max-reconnect-attempts",
        type=int,
        default=None,
        help="Maximum number of reconnection attempts (default: infinite)",
    )
    parser.set_defaults(partial=False, synchronous=False)
    namespace = _SynchronousArgs()
    args = parser.parse_args(namespace=namespace)
    applet_names = _prepare_applet_names(args.applet_names)

    if args.synchronous:
        synchronous_main(applet_names, args.partial)
    else:
        try:
            asyncio.run(main(applet_names, args.partial, args.max_reconnect_attempts))
        except KeyboardInterrupt:
            logger.info("Asynchronous connection cancelled manually.")


if __name__ == "__main__":
    cli()
