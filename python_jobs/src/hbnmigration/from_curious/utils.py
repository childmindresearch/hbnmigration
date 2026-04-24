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
REDCAP_TOKEN = redcap_variables.Tokens().pid625

# Standard REDCap metadata fetch parameters
METADATA_PARAMS = {
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
ALERTS_INSTRUMENT_FORM = "ra_alerts_child,ra_alerts_parent"
DEFAULT_EVENT_FOR_ALERTS = "admin_arm_1"

# Module-level cache for metadata
_METADATA_CACHE: dict[str, pd.DataFrame] = {}

STANDARD_FIELDS = {
    "record_id",
    "redcap_event_name",
    "redcap_repeat_instrument",
    "redcap_repeat_instance",
}
"""Common fields for REDCap metadata."""


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
    """Call Curious API."""
    response = requests.get(
        endpoint,
        headers=headers if headers else curious_variables.headers(tokens.access),
    )
    if response.status_code != requests.codes["okay"]:
        response.raise_for_status()
    result = response.json()["result"]
    if return_type:
        return cast(T, result)
    return result


def fetch_alerts_metadata(base_url: str) -> pd.DataFrame:
    """Fetch alerts instrument metadata from REDCap."""
    return fetch_api_data(
        base_url,
        redcap_variables.headers,
        {
            "token": REDCAP_TOKEN,
            "forms": ALERTS_INSTRUMENT_FORM,
            **METADATA_PARAMS,
        },
    )


def fetch_all_instruments_metadata(
    base_url: str, instrument_names: list[str]
) -> pd.DataFrame:
    """
    Fetch metadata for multiple instruments in a single API call.

    Parameters
    ----------
    base_url
        REDCap API base URL
    instrument_names
        List of instrument/form names

    Returns
    -------
    pd.DataFrame
        Combined metadata for all specified instruments

    """
    if not instrument_names:
        return pd.DataFrame()

    # Create cache key
    cache_key = ",".join(sorted(instrument_names))

    # Check cache first
    if cache_key in _METADATA_CACHE:
        logger.debug("Using cached metadata for: %s", cache_key)
        return _METADATA_CACHE[cache_key]

    logger.info("Fetching metadata for %d instruments", len(instrument_names))

    metadata = fetch_api_data(
        base_url,
        redcap_variables.headers,
        {
            "token": REDCAP_TOKEN,
            "forms": ",".join(instrument_names),
            **METADATA_PARAMS,
        },
    )

    # Cache the result
    _METADATA_CACHE[cache_key] = metadata

    return metadata


def fetch_instrument_metadata(base_url: str, instrument_name: str) -> pd.DataFrame:
    """
    Fetch instrument metadata from REDCap.

    Note: Consider using fetch_all_instruments_metadata for better performance
    when working with multiple instruments.

    Parameters
    ----------
    base_url
        REDCap API base URL
    instrument_name
        Name of the instrument/form

    Returns
    -------
    pd.DataFrame
        Metadata for the specified instrument

    """
    return fetch_all_instruments_metadata(base_url, [instrument_name])


def create_choice_lookup_bulk(
    metadata: pd.DataFrame,
) -> dict[tuple[str, str], str]:
    """
    Create lookup dictionary for mapping response values to REDCap indices.

    Works with metadata from multiple instruments.

    Parameters
    ----------
    metadata
        REDCap metadata DataFrame containing field definitions from instruments

    Returns
    -------
    dict[tuple[str, str], str]
        Mapping of (field_name, response_label) to REDCap index value

    """
    choice_lookup_tuples = [
        lookup_tuple
        for lookup_tuple in [
            item
            for _, row in metadata.iterrows()
            for item in response_index_reverse_lookup(row)
        ]
        if lookup_tuple
    ]
    return {
        lookup_tuple[0:2]: str(lookup_tuple[2]) for lookup_tuple in choice_lookup_tuples
    }


def create_choice_lookup(
    instrument_metadata: pd.DataFrame,
) -> dict[tuple[str, str], str]:
    """
    Create lookup dictionary for mapping response values to REDCap indices.

    Alias for create_choice_lookup_bulk for backwards compatibility.

    Parameters
    ----------
    instrument_metadata
        REDCap metadata DataFrame containing field definitions

    Returns
    -------
    dict[tuple[str, str], str]
        Mapping of (field_name, response_label) to REDCap index value

    """
    return create_choice_lookup_bulk(instrument_metadata)


def map_responses_to_indices_bulk(
    dfs: dict[str, pd.DataFrame],
    base_url: str,
    response_columns: Optional[dict[str, list[str]]] = None,
) -> dict[str, pd.DataFrame]:
    """
    Map response values to REDCap indices for multiple instruments at once.

    Parameters
    ----------
    dfs
        Dict mapping instrument_name to DataFrame with response data
    base_url
        REDCap API base URL
    response_columns
        Optional dict mapping instrument_name to list of response columns.
        If None, will auto-detect columns ending in '_response'

    Returns
    -------
    dict[str, pd.DataFrame]
        Dict mapping instrument_name to DataFrame with mapped values

    """
    if not dfs:
        return {}

    # Fetch all metadata at once
    instrument_names = list(dfs.keys())
    metadata = fetch_all_instruments_metadata(base_url, instrument_names)

    # Create unified choice lookup
    choice_lookup = create_choice_lookup_bulk(metadata)

    # DEBUG: Show what's in the choice lookup for problematic fields
    debug_fields = ["ysr_sr_1117_ysr_100_response", "ysr_sr_1117_ysr_100"]
    for debug_field in debug_fields:
        field_choices = {
            (fname, label): idx
            for (fname, label), idx in choice_lookup.items()
            if fname == debug_field
        }
        if field_choices:
            logger.info("=== Choice lookup for %s ===", debug_field)
            for (fname, label), idx in sorted(
                field_choices.items(), key=lambda x: x[1]
            ):
                logger.info("  Label: '%s' -> Index: '%s'", label, idx)

    if not choice_lookup:
        logger.debug("No choice mappings found for any instruments")
        return dfs

    logger.info(
        "Created choice lookup with %d mappings for %d instruments",
        len(choice_lookup),
        len(instrument_names),
    )

    # Process each dataframe
    results = {}
    for instrument_name, df in dfs.items():
        # Auto-detect response columns if not provided
        if response_columns is None or instrument_name not in response_columns:
            cols = [
                col
                for col in df.columns
                if col.endswith("_response") and col in df.columns
            ]
        else:
            cols = response_columns[instrument_name]

        if not cols:
            logger.debug("No response columns found for %s", instrument_name)
            results[instrument_name] = df
            continue

        logger.debug("Processing response columns for %s: %s", instrument_name, cols)

        # Map each response column
        result = df.copy()
        for col in cols:
            if col not in result.columns:
                continue

            # Extract field name from column
            field_name = col.replace("_response", "")

            # Create lookup key and map
            lookup_keys = [
                (field_name, str(val).strip().lower()) for val in result[col]
            ]
            result[col] = [
                choice_lookup.get(key, original)
                for key, original in zip(lookup_keys, result[col])
            ]

            mapped_count = sum(
                choice_lookup.get(key) is not None for key in lookup_keys
            )
            logger.debug(
                "Mapped %d/%d values for %s.%s",
                mapped_count,
                len(result),
                instrument_name,
                field_name,
            )

        results[instrument_name] = result

    return results


def map_responses_to_indices(
    df: pd.DataFrame,
    instrument_name: str,
    base_url: str,
    response_columns: Optional[list[str]] = None,
) -> pd.DataFrame:
    """
    Map response values to REDCap indices using choice lookup.

    Note: For processing multiple instruments, use map_responses_to_indices_bulk
    for better performance.

    Parameters
    ----------
    df
        DataFrame with response data
    instrument_name
        Name of the REDCap instrument
    base_url
        REDCap API base URL
    response_columns
        List of columns containing response values to map.
        If None, will attempt to auto-detect columns ending in '_response'

    Returns
    -------
    pd.DataFrame
        DataFrame with response values mapped to REDCap indices

    """
    result = map_responses_to_indices_bulk(
        {instrument_name: df},
        base_url,
        {instrument_name: response_columns} if response_columns else None,
    )
    return result[instrument_name]


def possible_alert_instruments(base_url: str) -> list[str]:
    """Return list of instruments that could trigger alerts."""
    return [
        col[:-7]
        for col in fetch_alerts_metadata(base_url).field_name.unique()
        if col.endswith("_alerts")
    ]


def get_instrument_event_mapping(base_url: str) -> dict[str, str]:
    """
    Get mapping of instruments to their designated events.

    Returns
    -------
    dict[str, str]
        Mapping of instrument_name to redcap_event_name

    """
    try:
        # Fetch instrument-event mapping from REDCap
        mapping = fetch_api_data(
            base_url,
            redcap_variables.headers,
            {
                "token": REDCAP_TOKEN,
                "content": "formEventMapping",
                "format": "json",
            },
        )
        if mapping.empty:
            logger.warning("No instrument-event mapping found in REDCap")
            return {}
        # Create dict mapping instrument name to event
        # If an instrument is used in multiple events, use the first one
        result = {}
        for _, row in mapping.iterrows():
            instrument = row.get("form")
            event = row.get("unique_event_name")
            if instrument and event and instrument not in result:
                result[instrument] = event
        return result
    except Exception as e:
        logger.warning("Could not fetch instrument-event mapping: %s", e)
        return {}


def get_alert_form_for_instrument(instrument_name: str) -> str:
    """
    Determine which alert form contains alerts for the given instrument.

    Parameters
    ----------
    instrument_name
        Name of the instrument (e.g., 'ace_p', 'cbcl')

    Returns
    -------
    str
        The alert form name ('ra_alerts_child' or 'ra_alerts_parent')

    """
    # Instruments that end with _p typically go to parent alerts
    # Instruments that end with _c or no suffix typically go to child alerts
    if instrument_name.endswith("_p"):
        return "ra_alerts_parent"
    if instrument_name.endswith("_c"):
        return "ra_alerts_child"
    # Default logic: check common patterns
    # This may need to be expanded based on your specific instruments
    parent_instruments = {"ace_p", "parent_baseline", "parent_followup"}
    if instrument_name in parent_instruments:
        return "ra_alerts_parent"
    return "ra_alerts_child"


def get_alert_field_event(base_url: str, instrument_name: str) -> str | None:
    """
    Get the event name for an alert field.

    Alert fields are stored on ra_alerts_parent or ra_alerts_child forms,
    which are typically on an admin event, NOT the same event as the data.

    Parameters
    ----------
    base_url
        REDCap API base URL
    instrument_name
        Name of the instrument (e.g., 'ace_p', 'cbcl')

    Returns
    -------
    str | None
        The event name for this instrument's alert field, or None if not found

    """
    # Determine which alert form this instrument uses
    alert_form = get_alert_form_for_instrument(instrument_name)
    # Get the instrument-event mapping
    instrument_events = get_instrument_event_mapping(base_url)
    # Look up the event for the ALERT FORM (not the data instrument)
    event = instrument_events.get(alert_form)
    if event:
        logger.info(
            "Alert field '%s_alerts' should use event '%s' (from form '%s')",
            instrument_name,
            event,
            alert_form,
        )
        return event
    logger.warning(
        "Could not determine event for alert form '%s' (instrument: '%s'), "
        "using default %s",
        alert_form,
        instrument_name,
        DEFAULT_EVENT_FOR_ALERTS,
    )
    return DEFAULT_EVENT_FOR_ALERTS


def get_field_to_event_mapping(base_url: str, field_names: list[str]) -> dict[str, str]:
    """
    Get mapping of field names to their REDCap event names.

    This fetches existing REDCap data and determines which event each field belongs to.

    Parameters
    ----------
    base_url
        REDCap API base URL
    field_names
        List of field names to look up

    Returns
    -------
    dict[str, str]
        Mapping of field_name to redcap_event_name

    """
    if not field_names:
        return {}
    # Fetch existing data for these fields
    params = {
        "token": REDCAP_TOKEN,
        "content": "record",
        "format": "json",
        "type": "flat",
        "fields": ",".join(field_names),
    }
    try:
        redcap_fields = fetch_api_data(
            base_url,
            redcap_variables.headers,
            params,
        )
        if redcap_fields.empty:
            logger.warning("No existing REDCap data found for fields: %s", field_names)
            return {}
        # Create mapping: field_name -> first event name found
        return cast(
            dict[str, str],
            redcap_fields.groupby("field_name")["redcap_event_name"].first().to_dict(),
        )
    except Exception as e:
        logger.warning("Could not fetch field-to-event mapping: %s", e)
        return {}


def get_activity(
    tokens: curious_variables.Tokens, activity_id: CuriousId
) -> CuriousActivity:
    """Get an activity from Curious."""
    return call_curious_api(
        tokens.endpoints.activity(activity_id), tokens, CuriousActivity
    )


def get_item(
    tokens: curious_variables.Tokens, activity_id: CuriousId, item_id: CuriousId
) -> CuriousItem:
    """Get an item from Curious."""
    activity = get_activity(tokens, activity_id)
    try:
        return next(item for item in activity["items"] if item["id"] == item_id)
    except StopIteration as stop_iteration:
        msg = f"Item {item_id} not found in {activity['name']} ({activity_id})."
        raise LookupError(msg) from stop_iteration


def alert_websocket_to_https(alert: CuriousAlert) -> CuriousAlertHttps:
    """Convert a `CuriousAlertWebsocket` to a `CuriousAlertHttps`."""
    return cast(CuriousAlertHttps, camelize(alert))


def map_mrns_to_records(
    redcap_alerts: pd.DataFrame,
    redcap_fields: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, str]]:
    """
    Map MRNs to record IDs and prepare lookups.

    Parameters
    ----------
    redcap_alerts
        DataFrame with alert data containing MRNs in 'record' column
    redcap_fields
        DataFrame with existing REDCap data for field validation

    Returns
    -------
    tuple
        (processed_alerts, mrn_lookup)
        - processed_alerts: Filtered alert DataFrame with event names populated
        - mrn_lookup: Maps MRN string to record ID integer

    """
    # Prepare data types
    redcap_alerts["record"] = (
        redcap_alerts["record"].str.replace(r"\D", "", regex=True).astype(str)
    )
    redcap_fields["record"] = redcap_fields["record"].astype(str)
    # Create lookups
    mrn_lookup = {
        str(k): str(v)
        for k, v in redcap_fields[redcap_fields["field_name"] == "mrn"]
        .set_index("value")["record"]
        .to_dict()
        .items()
    }
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


def create_row_hash(row_data: dict, fields: list[str]) -> str:
    """Create MD5 hash of row data for deduplication comparison."""
    hash_input = "|".join(str(row_data.get(field, "")) for field in sorted(fields))
    return hashlib.md5(hash_input.encode()).hexdigest()


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
    """
    Fetch existing REDCap data for deduplication comparison.

    Parameters
    ----------
    params : RedcapFetchParams
        Parameters for REDCap API fetch

    Returns
    -------
    pl.DataFrame
        Existing REDCap data or empty DataFrame

    """
    if not params.records or not params.fields:
        return pl.DataFrame()

    api_params = {
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
        existing_data = fetch_api_data(params.base_url, params.headers, api_params)
        # Handle both empty and None responses
        if existing_data is None or (
            isinstance(existing_data, pd.DataFrame) and existing_data.empty
        ):
            return pl.DataFrame()
        return pl.from_pandas(existing_data)
    except Exception as e:
        logger.warning("Could not fetch existing data: %s", e)
        return pl.DataFrame()


def remove_duplicate_rows(
    df: pl.DataFrame,
    token: str,
    base_url: str,
    headers: dict,
    instrument_name: str,
) -> tuple[pl.DataFrame, int]:
    """
    Remove rows that exactly match existing REDCap data using hash comparison.

    Parameters
    ----------
    df : pl.DataFrame
        New data to upload
    token : str
        REDCap API token
    base_url : str
        REDCap API base URL
    headers : dict
        Request headers
    instrument_name : str
        Name of instrument being processed

    Returns
    -------
    tuple[pl.DataFrame, int]
        (filtered_dataframe, num_duplicates_removed)

    """
    if "record_id" not in df.columns:
        return df, 0

    records = df["record_id"].cast(pl.Utf8).unique().to_list()
    data_fields = [col for col in df.columns if col not in STANDARD_FIELDS]

    if not data_fields:
        return df, 0

    event = ""
    if "redcap_event_name" in df.columns:
        events = df["redcap_event_name"].unique().to_list()
        if len(events) == 1:
            event = events[0]

    logger.info(
        "Fetching existing data for %s (%d records)...", instrument_name, len(records)
    )

    fetch_params = RedcapFetchParams(
        token=token,
        base_url=base_url,
        headers=headers,
        records=records,
        fields=data_fields,
        event=event,
    )
    existing_df = fetch_existing_redcap_data(fetch_params)

    if existing_df.is_empty():
        logger.info("No existing data found, uploading all %d rows", len(df))
        return df, 0

    # Create hash lookup for existing data
    existing_hashes = {}
    for row in existing_df.iter_rows(named=True):
        key = f"{row['record_id']}_{row.get('redcap_event_name', '')}"
        hash_val = create_row_hash(row, data_fields)
        existing_hashes[key] = hash_val

    # Filter new data by comparing hashes
    rows_to_keep = []
    for row in df.iter_rows(named=True):
        key = f"{row['record_id']}_{row.get('redcap_event_name', '')}"
        new_hash = create_row_hash(row, data_fields)

        if key not in existing_hashes or existing_hashes[key] != new_hash:
            rows_to_keep.append(row)

    df_filtered = (
        pl.DataFrame(rows_to_keep) if rows_to_keep else pl.DataFrame(schema=df.schema)
    )
    num_removed = len(df) - len(df_filtered)

    if num_removed > 0:
        logger.info(
            "Removed %d duplicate rows from %s (uploading %d new/changed rows)",
            num_removed,
            instrument_name,
            len(df_filtered),
        )

    return df_filtered, num_removed


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
    """
    Universal deduplication function supporting both pandas and polars DataFrames.

    Parameters
    ----------
    df : pd.DataFrame | pl.DataFrame
        Data to deduplicate
    token : str
        REDCap API token
    base_url : str
        REDCap API base URL
    headers : dict
        Request headers
    instrument_name : str
        Name of instrument

    Returns
    -------
    tuple[pd.DataFrame | pl.DataFrame, int]
        (deduplicated_dataframe, num_duplicates_removed)

    """
    is_pandas = isinstance(df, pd.DataFrame)

    # Convert to polars for processing
    df_pl = pl.from_pandas(df) if is_pandas else df

    # Deduplicate
    df_filtered, num_removed = remove_duplicate_rows(
        df_pl, token, base_url, headers, instrument_name
    )

    # Convert back to original format
    if is_pandas:
        return df_filtered.to_pandas(), num_removed
    return df_filtered, num_removed


def parse_dt(col_name: str) -> pl.Expr:
    """Parse an ISO 8601 datetime string column to Datetime('ms', 'UTC')."""
    return (
        pl.col(col_name)
        .str.replace("Z$", "")
        .str.strptime(pl.Datetime("ms"), "%Y-%m-%dT%H:%M:%S%.f")
        .dt.replace_time_zone("UTC")
    )
