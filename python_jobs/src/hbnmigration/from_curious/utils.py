"""Shared utilities for data processing from Curious to REDCap."""

from dataclasses import dataclass
import hashlib
import logging
from typing import cast, Optional, overload

from humps import camelize
import pandas as pd
import polars as pl
import requests

from .._config_variables import curious_variables, redcap_variables
from ..from_redcap.from_redcap import response_index_reverse_lookup
from ..utility_functions import (
    CuriousActivity,
    CuriousAlert,
    CuriousAlertHttps,
    CuriousId,
    CuriousItem,
    fetch_api_data,
    T,
)

logger = logging.getLogger(__name__)

REDCAP_TOKENS = redcap_variables.Tokens()
REDCAP_ENDPOINTS = redcap_variables.Endpoints()
"""Shared REDCap endpoints instance."""

METADATA_PARAMS: dict[str, str] = {
    "content": "metadata",
    "action": "export",
    "format": "csv",
    "type": "eav",
    "csvDelimiter": "",
    "rawOrLabel": "raw",
    "rawOrLabelHeaders": "raw",
    "exportCheckboxLabel": "false",
    "exportSurveyFields": "false",
    "exportDataAccessGroups": "false",
    "returnFormat": "csv",
}
"""Standard REDCap metadata fetch parameters."""

ALERTS_INSTRUMENT_FORM = "ra_alerts_child,ra_alerts_parent"
DEFAULT_EVENT_FOR_ALERTS = "admin_arm_1"

STANDARD_FIELDS = frozenset(
    {
        "record_id",
        "redcap_event_name",
        "redcap_repeat_instrument",
        "redcap_repeat_instance",
    }
)
"""Common REDCap metadata fields (immutable for fast lookup)."""

# Module-level caches
_METADATA_CACHE: dict[str, pd.DataFrame] = {}
_INSTRUMENT_EVENT_CACHE: dict[int, dict[str, str]] = {}
_ALERT_INSTRUMENTS_CACHE: dict[int, list[str]] = {}


# ============================================================================
# Token / API helpers
# ============================================================================


def get_redcap_token(pid: int) -> str:
    """Get REDCap token by *pid*."""
    return getattr(REDCAP_TOKENS, f"pid{pid}")


@overload
def call_curious_api(
    endpoint: str,
    tokens: curious_variables.Tokens,
    return_type: None = None,
    headers: Optional[dict[str, str]] = None,
) -> list | dict: ...


@overload
def call_curious_api(
    endpoint: str,
    tokens: curious_variables.Tokens,
    return_type: type[T],
    headers: Optional[dict[str, str]] = None,
) -> T: ...


def call_curious_api(
    endpoint: str,
    tokens: curious_variables.Tokens,
    return_type: Optional[type[T]] = None,
    headers: Optional[dict[str, str]] = None,
) -> T | list | dict:
    """Call Curious API and return the ``result`` key."""
    response = requests.get(
        endpoint,
        headers=headers or curious_variables.headers(tokens.access),
    )
    response.raise_for_status()
    result = response.json()["result"]
    return cast(T, result) if return_type else result


# ============================================================================
# Metadata fetching (cached)
# ============================================================================


def fetch_alerts_metadata(base_url: str, pid: int) -> pd.DataFrame:
    """Fetch alerts instrument metadata from REDCap."""
    return fetch_api_data(
        base_url,
        redcap_variables.headers,
        {
            "token": get_redcap_token(pid),
            "forms": ALERTS_INSTRUMENT_FORM,
            **METADATA_PARAMS,
        },
    )


def fetch_all_instruments_metadata(
    base_url: str,
    pid: int,
    instrument_names: list[str],
) -> pd.DataFrame:
    """Fetch metadata for multiple instruments in a single API call (cached)."""
    if not instrument_names:
        return pd.DataFrame()
    cache_key = f"{pid}:{','.join(sorted(instrument_names))}"
    if cache_key in _METADATA_CACHE:
        logger.debug("Using cached metadata for: %s", cache_key)
        return _METADATA_CACHE[cache_key]
    logger.info("Fetching metadata for %d instruments", len(instrument_names))
    metadata = fetch_api_data(
        base_url,
        redcap_variables.headers,
        {
            "token": get_redcap_token(pid),
            "forms": ",".join(instrument_names),
            **METADATA_PARAMS,
        },
    )
    _METADATA_CACHE[cache_key] = metadata
    return metadata


def fetch_instrument_metadata(
    base_url: str,
    pid: int,
    instrument_name: str,
) -> pd.DataFrame:
    """Fetch metadata (delegates to :func:`fetch_all_instruments_metadata`)."""
    return fetch_all_instruments_metadata(base_url, pid, [instrument_name])


# ============================================================================
# Choice lookups
# ============================================================================


def create_choice_lookup(metadata: pd.DataFrame) -> dict[tuple[str, str], str]:
    """
    Map ``(field_name, response_label) → REDCap index``.

    Works with metadata from one or many instruments.
    """
    return {
        t[0:2]: str(t[2])
        for _, row in metadata.iterrows()
        for t in response_index_reverse_lookup(row)
        if t
    }


# keep old name as alias
create_choice_lookup_bulk = create_choice_lookup


# ============================================================================
# Instrument / event mapping (cached)
# ============================================================================


def get_instrument_event_mapping(base_url: str, pid: int) -> dict[str, str]:
    """Return ``{instrument_name: first_event_name}`` (cached per *pid*)."""
    if pid in _INSTRUMENT_EVENT_CACHE:
        return _INSTRUMENT_EVENT_CACHE[pid]
    try:
        mapping = fetch_api_data(
            base_url,
            redcap_variables.headers,
            {
                "token": get_redcap_token(pid),
                "content": "formEventMapping",
                "format": "json",
            },
        )
        if mapping.empty:
            logger.warning("No instrument-event mapping found in REDCap")
            return {}
        result: dict[str, str] = {}
        for _, row in mapping.iterrows():
            inst, evt = row.get("form"), row.get("unique_event_name")
            if inst and evt:
                result.setdefault(inst, evt)
        _INSTRUMENT_EVENT_CACHE[pid] = result
        return result
    except Exception as e:
        logger.warning("Could not fetch instrument-event mapping: %s", e)
        return {}


def possible_alert_instruments(base_url: str, pid: int) -> list[str]:
    """Return instrument names that have an ``_alerts`` field (cached)."""
    if pid in _ALERT_INSTRUMENTS_CACHE:
        return _ALERT_INSTRUMENTS_CACHE[pid]
    instruments = [
        col[:-7]
        for col in fetch_alerts_metadata(base_url, pid).field_name.unique()
        if col.endswith("_alerts")
    ]
    _ALERT_INSTRUMENTS_CACHE[pid] = instruments
    return instruments


# ============================================================================
# Alert helpers
# ============================================================================

_PARENT_INSTRUMENTS = frozenset({"ace_p", "parent_baseline", "parent_followup"})


def alert_form_for_instrument(instrument_name: str) -> str:
    """Return ``'ra_alerts_parent'`` or ``'ra_alerts_child'``."""
    if instrument_name.endswith("_p") or instrument_name in _PARENT_INSTRUMENTS:
        return "ra_alerts_parent"
    return "ra_alerts_child"


def get_alert_field_event(
    base_url: str,
    pid: int,
    instrument_name: str,
) -> str | None:
    """
    Get the event where ``<instrument_name>_alerts`` lives.

    Alert fields sit on ``ra_alerts_parent`` / ``ra_alerts_child``, not on
    the data instrument itself.
    """
    form = alert_form_for_instrument(instrument_name)
    event = get_instrument_event_mapping(base_url, pid).get(form)
    if event:
        logger.info(
            "Alert '%s_alerts' → event '%s' (form '%s')", instrument_name, event, form
        )
        return event
    logger.warning(
        "No event for alert form '%s' (instrument '%s'), default %s",
        form,
        instrument_name,
        DEFAULT_EVENT_FOR_ALERTS,
    )
    return DEFAULT_EVENT_FOR_ALERTS


def get_field_to_event_mapping(
    base_url: str,
    pid: int,
    field_names: list[str],
) -> dict[str, str]:
    """Return ``{field_name: redcap_event_name}``."""
    if not field_names:
        return {}
    try:
        data = fetch_api_data(
            base_url,
            redcap_variables.headers,
            {
                "token": get_redcap_token(pid),
                "content": "record",
                "format": "json",
                "type": "flat",
                "fields": ",".join(field_names),
            },
        )
        if data.empty:
            return {}
        return cast(
            dict[str, str],
            data.groupby("field_name")["redcap_event_name"].first().to_dict(),
        )
    except Exception as e:
        logger.warning("Could not fetch field-to-event mapping: %s", e)
        return {}


# ============================================================================
# REDCap record fetching for alert status checks
# ============================================================================


def get_redcap_records_for_instrument(
    instrument: str,
    records: list[str],
    pid: int,
    event: str = "",
) -> dict[str, dict[str, str]]:
    """Get existing REDCap data for *records* and *instrument*."""
    token = get_redcap_token(pid)
    md = fetch_api_data(
        REDCAP_ENDPOINTS.base_url,
        redcap_variables.headers,
        {"token": token, "content": "metadata", "format": "json", "forms": instrument},
    )
    if md.empty:
        logger.warning("No metadata for instrument: %s", instrument)
        return {}
    fields = md["field_name"].tolist() if "field_name" in md.columns else []
    alert_field = f"{instrument}_alerts"
    if alert_field not in fields:
        return {}
    params: dict[str, str] = {
        "token": token,
        "content": "record",
        "format": "json",
        "type": "flat",
        "records": ",".join(records),
        "fields": f"record_id,{alert_field}",
    }
    if event:
        params["events"] = event
    try:
        resp = requests.post(
            REDCAP_ENDPOINTS.base_url,
            data=params,
            headers=redcap_variables.headers,
        )
        resp.raise_for_status()
        return {r["record_id"]: r for r in resp.json() if r.get("record_id")}
    except Exception as e:
        logger.warning("Could not fetch REDCap data for %s: %s", instrument, e)
        return {}


# ============================================================================
# Curious activity / item helpers
# ============================================================================


def get_activity(
    tokens: curious_variables.Tokens,
    activity_id: CuriousId,
) -> CuriousActivity:
    """Get an activity from Curious."""
    return call_curious_api(
        tokens.endpoints.activity(activity_id), tokens, CuriousActivity
    )


def get_item(
    tokens: curious_variables.Tokens,
    activity_id: CuriousId,
    item_id: CuriousId,
) -> CuriousItem:
    """Get a single item from a Curious activity."""
    activity = get_activity(tokens, activity_id)
    try:
        return next(i for i in activity["items"] if i["id"] == item_id)
    except StopIteration as exc:
        msg = f"Item {item_id} not found in {activity['name']} ({activity_id})."
        raise LookupError(msg) from exc


def alert_websocket_to_https(alert: CuriousAlert) -> CuriousAlertHttps:
    """Convert a ``CuriousAlertWebsocket`` to ``CuriousAlertHttps``."""
    return cast(CuriousAlertHttps, camelize(alert))


# ============================================================================
# MRN mapping
# ============================================================================


def map_mrns_to_records(
    redcap_alerts: pd.DataFrame,
    redcap_fields: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, str]]:
    """
    Map MRNs to record IDs; return ``(processed_alerts, mrn_lookup)``.

    ``mrn_lookup`` maps MRN strings to record-ID strings.
    """
    redcap_alerts["record"] = (
        redcap_alerts["record"].str.replace(r"\D", "", regex=True).astype(str)
    )
    redcap_fields["record"] = redcap_fields["record"].astype(str)
    mrn_lookup = {
        str(k): str(v)
        for k, v in (
            redcap_fields[redcap_fields["field_name"] == "mrn"]
            .set_index("value")["record"]
            .to_dict()
            .items()
        )
    }
    record_events = cast(
        dict[str, str],
        redcap_fields.groupby("field_name")["redcap_event_name"].first().to_dict(),
    )
    result = redcap_alerts.loc[redcap_alerts["field_name"] != "mrn"].copy()
    result = result[result["field_name"].isin(redcap_fields["field_name"])]
    result["redcap_event_name"] = result["field_name"].map(record_events)
    return result, mrn_lookup


# ============================================================================
# Deduplication
# ============================================================================


def _row_hash(row: dict, sorted_fields: list[str]) -> str:
    """MD5 hash of *row* values for pre-sorted *sorted_fields*."""
    return hashlib.md5(
        "|".join(str(row.get(f, "")) for f in sorted_fields).encode(),
    ).hexdigest()


# Keep old public name
create_row_hash = _row_hash


@dataclass
class RedcapFetchParams:
    """Parameters for fetching REDCap data."""

    token: str
    base_url: str
    headers: dict
    records: list[str]
    fields: list[str]
    event: str = ""


def fetch_existing_redcap_data(params: RedcapFetchParams) -> pl.DataFrame:
    """Fetch existing REDCap data for deduplication comparison."""
    if not params.records or not params.fields:
        return pl.DataFrame()
    api_params: dict[str, str] = {
        "token": params.token,
        "content": "record",
        "format": "csv",
        "type": "flat",
        "records": ",".join(params.records),
        "fields": ",".join(["record_id", *params.fields]),
    }
    if params.event:
        api_params["events"] = params.event
    try:
        existing = fetch_api_data(params.base_url, params.headers, api_params)
        if existing is None or (isinstance(existing, pd.DataFrame) and existing.empty):
            return pl.DataFrame()
        return pl.from_pandas(existing)
    except Exception as e:
        logger.warning("Could not fetch existing data: %s", e)
        return pl.DataFrame()


def _build_existing_hashes(
    existing_df: pl.DataFrame,
    sorted_fields: list[str],
) -> dict[str, str]:
    """Build ``{record_event_key: hash}`` from existing REDCap data."""
    return {
        f"{row['record_id']}_{row.get('redcap_event_name', '')}": _row_hash(
            row, sorted_fields
        )
        for row in existing_df.iter_rows(named=True)
    }


def remove_duplicate_rows(
    df: pl.DataFrame,
    token: str,
    base_url: str,
    headers: dict,
    instrument_name: str,
) -> tuple[pl.DataFrame, int]:
    """Remove rows that exactly match existing REDCap data (hash comparison)."""
    if "record_id" not in df.columns:
        return df, 0
    data_fields = [c for c in df.columns if c not in STANDARD_FIELDS]
    if not data_fields:
        return df, 0
    sorted_fields = sorted(data_fields)
    records = df["record_id"].cast(pl.Utf8).unique().to_list()
    event = ""
    if "redcap_event_name" in df.columns:
        events = df["redcap_event_name"].unique().to_list()
        if len(events) == 1:
            event = events[0]

    logger.info(
        "Fetching existing data for %s (%d records)…", instrument_name, len(records)
    )
    existing_df = fetch_existing_redcap_data(
        RedcapFetchParams(token, base_url, headers, records, data_fields, event),
    )
    if existing_df.is_empty():
        logger.info("No existing data, uploading all %d rows", len(df))
        return df, 0

    hashes = _build_existing_hashes(existing_df, sorted_fields)
    kept = [
        row
        for row in df.iter_rows(named=True)
        if (
            (k := f"{row['record_id']}_{row.get('redcap_event_name', '')}")
            not in hashes
            or hashes[k] != _row_hash(row, sorted_fields)
        )
    ]
    filtered = pl.DataFrame(kept) if kept else pl.DataFrame(schema=df.schema)
    removed = len(df) - len(filtered)
    if removed:
        logger.info(
            "Removed %d duplicates from %s (uploading %d)",
            removed,
            instrument_name,
            len(filtered),
        )
    return filtered, removed


@overload
def deduplicate_dataframe(
    df: pd.DataFrame,
    token: str,
    base_url: str,
    headers: dict,
    instrument_name: str,
) -> tuple[pd.DataFrame, int]: ...


@overload
def deduplicate_dataframe(
    df: pl.DataFrame,
    token: str,
    base_url: str,
    headers: dict,
    instrument_name: str,
) -> tuple[pl.DataFrame, int]: ...


def deduplicate_dataframe(
    df: pd.DataFrame | pl.DataFrame,
    token: str,
    base_url: str,
    headers: dict,
    instrument_name: str,
) -> tuple[pd.DataFrame | pl.DataFrame, int]:
    """Universal deduplication supporting both pandas and polars."""
    is_pandas = isinstance(df, pd.DataFrame)
    df_pl = pl.from_pandas(df) if is_pandas else df
    filtered, n = remove_duplicate_rows(
        df_pl, token, base_url, headers, instrument_name
    )
    return (filtered.to_pandas(), n) if is_pandas else (filtered, n)


# ============================================================================
# Datetime parsing
# ============================================================================


def parse_dt(col_name: str) -> pl.Expr:
    """Parse an ISO 8601 string column to ``Datetime('ms', 'UTC')``."""
    return (
        pl.col(col_name)
        .str.replace("Z$", "")
        .str.strptime(pl.Datetime("ms"), "%Y-%m-%dT%H:%M:%S%.f")
        .dt.replace_time_zone("UTC")
    )
