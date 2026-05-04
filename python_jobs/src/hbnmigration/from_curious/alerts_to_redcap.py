"""Monitor Curious alerts and send them to REDCap."""

import argparse
import asyncio
from contextlib import asynccontextmanager
from datetime import date
import json
import logging
import re
from typing import Any, AsyncIterator, cast, Optional

import pandas as pd
import requests
import websockets
from websockets.exceptions import (
    ConnectionClosedError,
    ConnectionClosedOK,
    InvalidStatus,
)

from .._config_variables import curious_variables, redcap_variables
from ..exceptions import NoData
from ..from_redcap.config import FieldList
from ..from_redcap.from_redcap import fetch_data
from ..utility_functions import (
    compute_content_hash,
    create_composite_cache_key,
    CuriousAlert,
    CuriousAlertHttps,
    CuriousAlertWebsocket,
    DataCache,
    initialize_logging,
    log_cache_statistics,
    redcap_api_push,
)
from ..utility_functions.logging import log_root_path
from ..utility_functions.teams import send_alert
from .config import curious_authenticate
from .utils import (
    alert_form_for_instrument,
    alert_websocket_to_https,
    call_curious_api,
    create_choice_lookup,
    deduplicate_dataframe,
    fetch_alerts_metadata,
    get_redcap_token,
    map_mrns_to_records,
    REDCAP_ENDPOINTS,
)

initialize_logging()
logger = logging.getLogger(__name__)

# ============================================================================
# Constants
# ============================================================================

ALERT_FIELD_PATTERN = r"alerts_([^_]+(?:_[^_]+)?)_\d+"

PID_625_TOKEN = get_redcap_token(625)
"""Token for PID 625 (alerts always go here)."""

# WebSocket configuration
WS_RECONNECT_DELAY = 5
WS_MAX_RECONNECT_ATTEMPTS: int | None = None
WS_PING_INTERVAL = 20
WS_PING_TIMEOUT = 10
WS_CLOSE_TIMEOUT = 5


# ============================================================================
# Type Definitions
# ============================================================================


class _SynchronousArgs(argparse.Namespace):
    """Typehints for CLI args."""

    synchronous: bool
    partial: bool
    max_reconnect_attempts: Optional[int]


# ============================================================================
# Cache Key Utilities
# ============================================================================


def create_alert_cache_key(alert_id: str, message: str) -> str:
    """Create a unique cache key for an alert."""
    return create_composite_cache_key(alert_id, compute_content_hash(message, length=8))


# ============================================================================
# Authentication & Connection
# ============================================================================


@asynccontextmanager
async def connect_to_websocket(
    token: str,
    uri: str,
) -> AsyncIterator[websockets.ClientConnection]:
    """Connect to a websocket with auth token and proper configuration."""
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
    """Return *applet_names* or all configured applets."""
    return list(curious_variables.applets.keys()) if not applet_names else applet_names


# ============================================================================
# Alert Parsing
# ============================================================================


def _log_invalid_alert_fields(fields: list[str]) -> None:
    """
    Log alert field names that don't exist in REDCap metadata.

    Appends new fields to a daily log file and sends a Teams alert
    when previously-unseen fields are encountered.
    """
    log_dir = log_root_path() / "invalid_alert_fields"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"{date.today().isoformat()}.txt"

    # Read existing entries
    existing: set[str] = set()
    if log_file.exists():
        existing = set(log_file.read_text().splitlines())

    # Determine which are new
    new_fields = [f for f in fields if f not in existing]
    if not new_fields:
        logger.debug("Invalid alert fields already logged: %s", fields)
        return

    # Append new fields to log
    with log_file.open("a") as f:
        for field in new_fields:
            f.write(f"{field}\n")

    logger.warning("Invalid alert fields logged: %s", new_fields)

    # Send Teams alert
    send_alert(
        f"⚠️ Curious → REDCap Alerts: {len(new_fields)} invalid field name(s) "
        f"detected.\n\n"
        f"The following alert item names from Curious do not match any "
        f"REDCap field in the alerts instrument:\n"
        + "\n".join(f"• `{f}`" for f in new_fields)
        + "\n\nThis likely means the Curious item name is inconsistent with "
        f"the REDCap data dictionary. Check the item naming in Curious.\n\n"
        f"Log file: `{log_file}`",
        "Send webhook alerts to 🔴 MS Fabric - Failures",
    )


def _parse_alert_message(message: str) -> tuple[str, str]:
    """Parse alert message; return ``(answer, item_field_name)``."""
    _color, remainder = message.split(': "', 1)
    answer, remainder = remainder.split('" to', 1)
    _, item = remainder.rsplit(" ", 1)
    return answer, f"alerts_{item.lower()}"


def parse_alert(alert: CuriousAlert) -> pd.DataFrame:
    """
    Parse a Curious alert into REDCap EAV format.

    Columns: ``record``, ``field_name``, ``value``, ``redcap_event_name``
    (``record``, ``value``, ``redcap_event_name`` need further processing).
    """
    columns = ["record", "field_name", "value", "redcap_event_name"]
    alert = alert_websocket_to_https(alert)
    answer, item = _parse_alert_message(alert["message"])
    data: list[tuple[str, str, Any, None]] = [
        (alert["secretId"], field, val, None)
        for field, val in [("mrn", alert["secretId"]), (item, answer)]
    ]
    return pd.DataFrame(data, columns=columns)


# ============================================================================
# REDCap Data Processing
# ============================================================================


def toggle_alerts(result: pd.DataFrame) -> pd.DataFrame:
    """Add ``{instrument}_alerts = 'yes'`` for each respondent + instrument."""
    if result.empty:
        return result
    instruments = result["field_name"].str.extract(ALERT_FIELD_PATTERN, expand=False)
    summary = result[instruments.notna()].copy()
    summary["field_name"] = instruments[instruments.notna()] + "_alerts"
    summary = summary.drop_duplicates(["record", "field_name", "redcap_event_name"])
    summary["value"] = "yes"
    return pd.concat([result, summary], ignore_index=True)


def _normalize_instrument_name(instrument_part: str) -> str:
    """Convert an extracted instrument part to the standard alert form name."""
    if "parent" in instrument_part or instrument_part.endswith("_p"):
        return "ra_alerts_parent"
    if "child" in instrument_part or instrument_part.endswith("_c"):
        return "ra_alerts_child"
    if instrument_part.startswith("ra_alerts"):
        return instrument_part
    # Delegate to the shared helper for edge cases
    return alert_form_for_instrument(instrument_part)


def derive_instrument_name(result: pd.DataFrame) -> str:
    """
    Derive the alert-form instrument name from alert field columns.

    Returns ``'ra_alerts_parent'`` or ``'ra_alerts_child'``
    (defaults to ``'ra_alerts_parent'``).
    """
    field_col = result.get("field_name", pd.Series(dtype=str))
    for pattern in (r"alerts_([a-z_]+)_\d+", r"^([a-z_]+)_alerts$"):
        for val in field_col:
            if pd.notna(val):
                m = re.search(pattern, str(val))
                if m:
                    return _normalize_instrument_name(m.group(1))
    # Column-name fallback
    for col in result.columns:
        if col.endswith("_alerts"):
            return _normalize_instrument_name(col.replace("_alerts", ""))
        if col.startswith("alerts_"):
            parts = col.split("_")
            if len(parts) >= 2:  # noqa: PLR2004
                return _normalize_instrument_name(parts[1])
    logger.warning("Could not derive instrument name, defaulting to 'ra_alerts_parent'")
    return "ra_alerts_parent"


def _validate_alert_fields(
    redcap_alerts: pd.DataFrame,
    alerts_instrument: pd.DataFrame,
) -> tuple[pd.DataFrame, list[str]]:
    """
    Validate alert field names against REDCap metadata.

    Returns
    -------
    tuple[pd.DataFrame, list[str]]
        (filtered alerts with only valid fields + mrn, valid field names)

    """
    known = set(alerts_instrument["field_name"].unique())
    data_fields = [f for f in redcap_alerts["field_name"].unique() if f != "mrn"]
    valid = [f for f in data_fields if f in known]
    invalid = [f for f in data_fields if f not in known]
    if invalid:
        _log_invalid_alert_fields(invalid)
    filtered = redcap_alerts[redcap_alerts["field_name"].isin([*valid, "mrn"])].copy()
    return filtered, valid


def _fetch_redcap_context(valid_fields: list[str]) -> pd.DataFrame | None:
    """
    Fetch MRN data and (optionally) alert field data from REDCap.

    Returns combined DataFrame or ``None`` if no MRN data exists.
    """
    mrn_data = fetch_data(PID_625_TOKEN, "mrn", all_or_any="any")
    if mrn_data.empty:
        logger.warning("No MRN data found in REDCap")
        return None
    try:
        field_data = fetch_data(
            PID_625_TOKEN,
            str(FieldList(valid_fields)),
            all_or_any="any",
        )
        return pd.concat([mrn_data, field_data], ignore_index=True)
    except NoData:
        logger.debug(
            "No existing data for alert fields %s (expected for new alerts)",
            valid_fields,
        )
        return mrn_data


def _map_and_transform(
    redcap_alerts: pd.DataFrame,
    redcap_fields: pd.DataFrame,
    alerts_instrument: pd.DataFrame,
) -> pd.DataFrame:
    """
    Map MRNs → record IDs, convert response values to indices, drop unmapped.

    Returns the transformed DataFrame (may be empty).
    """
    result, mrn_lookup = map_mrns_to_records(redcap_alerts, redcap_fields)
    if result.empty:
        logger.warning("No data after MRN mapping")
        return result

    choice_lookup = create_choice_lookup(alerts_instrument)
    result["lookup_key"] = list(
        zip(result["field_name"], result["value"].str.strip().str.lower()),
    )
    result["value"] = result["lookup_key"].map(choice_lookup).fillna(result["value"])
    result["record"] = result["record"].map(mrn_lookup)

    unmapped = result["record"].isna()
    if unmapped.any():
        logger.warning(
            "Could not map %d alert rows to record IDs (MRNs: %s)",
            unmapped.sum(),
            redcap_alerts.loc[redcap_alerts["field_name"] == "mrn", "value"]
            .unique()
            .tolist(),
        )

    return result.dropna(subset=["record"]).drop(columns="lookup_key")


def process_alerts_for_redcap(
    pid: int,
    redcap_alerts: pd.DataFrame,
    partial_redcap_landing: bool = False,
) -> pd.DataFrame:
    """
    Process alerts for REDCap push.

    Fetches metadata, validates field names, maps MRNs → record IDs,
    converts response values to REDCap indices, and toggles alert flags.
    """
    empty = pd.DataFrame(columns=redcap_alerts.columns)
    if redcap_alerts.empty:
        return empty

    alerts_instrument = fetch_alerts_metadata(REDCAP_ENDPOINTS.base_url, pid)
    if alerts_instrument.empty:
        logger.warning("No alerts metadata found for PID %d", pid)
        return empty

    redcap_alerts, valid_fields = _validate_alert_fields(
        redcap_alerts,
        alerts_instrument,
    )
    if not valid_fields or redcap_alerts.empty:
        logger.warning("No valid alert fields found in metadata")
        return empty

    if partial_redcap_landing:
        known = set(alerts_instrument["field_name"].unique())
        valid_fields = [f for f in valid_fields if f in known]

    redcap_fields = _fetch_redcap_context(valid_fields)
    if redcap_fields is None:
        return empty

    result = _map_and_transform(redcap_alerts, redcap_fields, alerts_instrument)
    if result.empty:
        return empty

    return toggle_alerts(result)


def push_alerts_to_redcap(result: pd.DataFrame) -> None:
    """Push processed alerts to REDCap (PID 625) with deduplication."""
    if result.empty:
        logger.info("No alerts to push (empty DataFrame)")
        return

    instrument_name = derive_instrument_name(result)
    logger.info("Derived instrument name: %s", instrument_name)

    result, n_dup = deduplicate_dataframe(
        result,
        PID_625_TOKEN,
        REDCAP_ENDPOINTS.base_url,
        redcap_variables.headers,
        instrument_name,
    )
    if result.empty:
        logger.info("All alert rows are duplicates, skipping")
        return
    if n_dup:
        logger.info("Removed %d duplicate alert rows", n_dup)

    try:
        redcap_api_push(
            result,
            PID_625_TOKEN,
            REDCAP_ENDPOINTS.base_url,
            redcap_variables.headers,
        )
        logger.info("%d rows updated for alerts in PID 625.", result.shape[0])
    except Exception:
        logger.exception("Pushing alerts from Curious to REDCap failed.")
        raise


# ============================================================================
# Alert Processing Pipeline
# ============================================================================


def _process_single_alert(
    pid: int,
    alert: CuriousAlert,
    partial_redcap_landing: bool = False,
) -> pd.DataFrame | None:
    """Process one alert; return result or ``None`` if invalid."""
    if alert.get("type") != "answer":
        logger.debug("Skipping non-answer type: %s", alert.get("type"))
        return None
    redcap_alert = parse_alert(alert)
    result = (
        process_alerts_for_redcap(pid, redcap_alert, partial_redcap_landing)
        if not redcap_alert.empty
        else redcap_alert
    )
    if result.empty:
        logger.warning("No valid data for alert ID: %s", alert.get("id"))
        return None
    return result


def _handle_alert_errors(message: str, error: Exception) -> None:
    """Centralised error handling for alert processing."""
    if isinstance(error, json.JSONDecodeError):
        logger.exception("Failed to parse JSON: %s", message)
    elif isinstance(error, KeyError):
        logger.exception("Missing expected field in alert")
    else:
        logger.exception("Error processing alert: %s", message)


# ============================================================================
# WebSocket Listener
# ============================================================================


async def websocket_listener(
    websocket: websockets.ClientConnection,
    partial_redcap_landing: bool = False,
) -> None:
    """Listen to websocket and process alerts (PID 625 only)."""
    pid = 625
    try:
        async for message in websocket:
            logger.info("Received alert: %s", message)
            try:
                alert: CuriousAlertWebsocket = json.loads(message)
                result = _process_single_alert(pid, alert, partial_redcap_landing)
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


async def _reconnect_loop(
    applet_name: str,
    uri: str,
    partial_redcap_landing: bool,
    max_attempts: int | None,
) -> None:
    """Connect / reconnect to websocket in a loop."""
    attempt = 0
    tokens = curious_authenticate(applet_name)
    while max_attempts is None or attempt < max_attempts:
        try:
            if attempt:
                logger.info(
                    "Reconnect attempt %d%s",
                    attempt,
                    f" of {max_attempts}" if max_attempts else "",
                )
            async with connect_to_websocket(tokens.access, uri) as ws:
                if attempt:
                    logger.info("Reconnected to WebSocket")
                attempt = 0
                await websocket_listener(ws, partial_redcap_landing)
                break
        except ConnectionClosedError:
            attempt += 1
            if max_attempts is not None and attempt >= max_attempts:
                logger.exception("Max reconnect attempts reached.")
                raise
            logger.warning("Connection lost. Reconnecting in %ds…", WS_RECONNECT_DELAY)
            await asyncio.sleep(WS_RECONNECT_DELAY)
        except InvalidStatus as e:
            logger.exception("WebSocket failed with status %s", e.response.status_code)
            if e.response.status_code == requests.codes["unauthorized"]:
                logger.warning("Re-authenticating…")
                try:
                    tokens = curious_authenticate(applet_name)
                except Exception:
                    logger.exception("Re-authentication failed.")
                    raise
            attempt += 1
            if max_attempts is not None and attempt >= max_attempts:
                logger.exception("Max reconnect attempts reached.")
                raise
            await asyncio.sleep(WS_RECONNECT_DELAY)
        except asyncio.CancelledError:
            logger.info("Operation cancelled")
            raise
        except KeyboardInterrupt:
            logger.info("Listener cancelled manually")
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
    max_attempts: int | None = None,
) -> None:
    """Send Curious alerts to REDCap (async, PID 625 only)."""
    endpoints = curious_variables.Endpoints(protocol="wss")
    for applet_name in applet_names:
        await _reconnect_loop(
            applet_name, endpoints.alerts, partial_redcap_landing, max_attempts
        )


def synchronous_main(
    applet_names: list[str],
    partial_redcap_landing: bool = False,
) -> None:
    """Send Curious alerts to REDCap (synchronous REST, PID 625 only)."""
    cache = DataCache("curious_alerts_to_redcap", ttl_minutes=2)
    pid = 625
    for applet_name in applet_names:
        tokens = curious_authenticate(applet_name)
        results = call_curious_api(
            tokens.endpoints.alerts,
            tokens,
            return_type=list[CuriousAlertHttps],
        )
        unprocessed: list[tuple[CuriousAlertHttps, str]] = []
        for alert in results:
            ck = create_alert_cache_key(alert.get("id", ""), alert.get("message", ""))
            if cache.is_processed(ck):
                logger.debug("Skipping cached alert: %s", ck)
            else:
                unprocessed.append((alert, ck))
        if not unprocessed:
            logger.info("All alerts already processed.")
            continue
        logger.info(
            "Processing %d new alerts (skipped %d cached)",
            len(unprocessed),
            len(results) - len(unprocessed),
        )
        redcap_alerts = pd.concat([parse_alert(a) for a, _ in unprocessed])
        result = process_alerts_for_redcap(pid, redcap_alerts, partial_redcap_landing)
        push_alerts_to_redcap(result)
        for alert, ck in unprocessed:
            cache.mark_processed(
                ck, metadata={"alert_id": alert.get("id", ""), "processed": True}
            )
        log_cache_statistics(cache, logger)


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
        help="Max reconnection attempts (default: infinite)",
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
            logger.info("Async connection cancelled manually.")


if __name__ == "__main__":
    cli()
