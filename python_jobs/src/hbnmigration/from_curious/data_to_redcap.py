"""Send Curious data to REDCap."""

import csv
from datetime import datetime
import hashlib
import logging
import os
from pathlib import Path
import re
import sys
from tempfile import TemporaryDirectory
import time
from typing import Literal

import pandas as pd
import polars as pl
import requests

from mindlogger_data_export.mindlogger import MindloggerData
from mindlogger_data_export.outputs import NamedOutput, RedcapImportFormat

from .._config_variables import curious_variables, redcap_variables
from ..config import Config
from ..exceptions import NoData
from ..from_redcap.config import FieldList
from ..from_redcap.from_redcap import fetch_data
from ..utility_functions import (
    CliOptions,
    DataCache,
    Endpoints,
    fetch_api_data,
    get_recent_time_window,
    get_redcap_event_names,
    initialize_logging,
    InstrumentRowCount,
    Results,
    tsx,
    YESTERDAY,
)
from .utils import (
    deduplicate_dataframe,
    get_alert_field_event,
    map_mrns_to_records,
    possible_alert_instruments,
    REDCAP_TOKEN,
    STANDARD_FIELDS,
)

initialize_logging()
logger = logging.getLogger(__name__)

APPLET_CREDENTIALS = curious_variables.AppletCredentials()
"""Initialized credentials."""

ENDPOINTS: dict[Literal["Curious", "REDCap"], Endpoints] = {
    "Curious": curious_variables.Endpoints(),
    "REDCap": redcap_variables.Endpoints(),
}
"""Initialized endpoints"""


def _extract_field_names_from_outputs(outputs: list[NamedOutput]) -> set[str]:
    """
    Extract all unique field names from formatted outputs.

    This identifies which fields need metadata for choice mapping.

    Parameters
    ----------
    outputs
        List of formatted outputs

    Returns
    -------
    set[str]
        Set of field names that end with _response, _index, or _score

    """
    all_fields = set()
    for output in outputs:
        df = output.output
        # Get fields that might need choice mapping
        for col in df.columns:
            # Remove instrument prefix to get base field name
            # e.g., "ysr_sr_1117_ysr_100_response" -> "ysr_sr_1117_ysr_100_response"
            if col.endswith(("_response", "_index", "_score")):
                all_fields.add(col)

    return all_fields


def _fetch_redcap_metadata_for_fields(field_names: set[str]) -> pl.DataFrame | None:
    """
    Fetch REDCap metadata for specific fields only.

    This is more efficient than fetching all metadata when we only need
    a subset of fields.

    Parameters
    ----------
    field_names
        Set of field names to fetch metadata for

    Returns
    -------
    pl.DataFrame | None
        REDCap metadata as polars DataFrame, or None if empty

    """
    if not field_names:
        logger.warning("No field names provided for metadata fetch")
        return None

    logger.info("Fetching REDCap metadata for %d specific fields...", len(field_names))

    # REDCap API requires fields as comma-separated string
    fields_param = ",".join(sorted(field_names))

    try:
        metadata = fetch_api_data(
            ENDPOINTS["REDCap"].base_url,
            redcap_variables.headers,
            {
                "token": REDCAP_TOKEN,
                "content": "metadata",
                "format": "csv",
                "fields": fields_param,
            },
        )

        if metadata.empty:
            logger.warning("No metadata returned for requested fields")
            return None

        # Convert to polars
        redcap_metadata_pl = pl.from_pandas(metadata)

        logger.info(
            "Loaded REDCap metadata for %d fields (requested %d)",
            len(redcap_metadata_pl),
            len(field_names),
        )

        return redcap_metadata_pl

    except Exception as e:
        logger.warning("Error fetching field-specific metadata: %s", e)
        logger.info("Falling back to full metadata fetch...")
        return _fetch_redcap_metadata_for_formatting()


def _fetch_redcap_metadata_for_formatting() -> pl.DataFrame | None:
    """
    Fetch all REDCap metadata for choice value mapping.

    This is a fallback when field-specific fetching fails.

    Returns
    -------
    pl.DataFrame | None
        REDCap metadata as polars DataFrame, or None if empty

    """
    logger.info("Fetching all REDCap metadata for choice value mapping...")
    all_instruments_metadata = fetch_api_data(
        ENDPOINTS["REDCap"].base_url,
        redcap_variables.headers,
        {
            "token": REDCAP_TOKEN,
            "content": "metadata",
            "format": "csv",
        },
    )

    # Convert to polars for passing to formatter
    redcap_metadata_pl = (
        pl.from_pandas(all_instruments_metadata)
        if not all_instruments_metadata.empty
        else None
    )

    if redcap_metadata_pl is not None:
        logger.info("Loaded REDCap metadata for %d fields", len(redcap_metadata_pl))

    return redcap_metadata_pl


def _filter_parent_records_for_account_created(df: pl.DataFrame) -> pl.DataFrame:
    """
    Strip _P suffix from record IDs for curious_account_created instrument.

    Parameters
    ----------
    df
        DataFrame with target_user_secret_id column

    Returns
    -------
    pl.DataFrame
        Filtered DataFrame with _P suffix removed from record IDs

    """
    with_p = df.filter(
        pl.col("target_user_secret_id").cast(pl.Utf8).str.ends_with("_P")
    )
    without_p = df.filter(
        ~pl.col("target_user_secret_id").cast(pl.Utf8).str.ends_with("_P")
    )

    if len(with_p) > 0:
        # Strip _P suffix using string replacement
        with_p = with_p.with_columns(
            pl.col("target_user_secret_id")
            .cast(pl.Utf8)
            .str.replace(r"_P$", "")
            .alias("target_user_secret_id")
        )
        logger.info(
            "Stripped '_P' suffix from %d records in curious_account_created",
            len(with_p),
        )
        return pl.concat([without_p, with_p])

    return df


def _filter_parent_records_from_instrument(
    df: pl.DataFrame, instrument_name: str
) -> pl.DataFrame:
    """
    Filter out records with _P suffix from regular instruments.

    Parameters
    ----------
    df
        DataFrame with target_user_secret_id column
    instrument_name
        Name of the instrument being processed

    Returns
    -------
    pl.DataFrame
        Filtered DataFrame without _P records

    """
    with_p = df.filter(
        pl.col("target_user_secret_id").cast(pl.Utf8).str.ends_with("_P")
    )
    df_filtered = df.filter(
        ~pl.col("target_user_secret_id").cast(pl.Utf8).str.ends_with("_P")
    )

    if len(with_p) > 0:
        ignored_records = with_p["target_user_secret_id"].cast(pl.Utf8).to_list()
        logger.info(
            "Ignored %d records with '_P' suffix in %s: %s",
            len(with_p),
            instrument_name,
            ", ".join(str(r) for r in ignored_records),
        )

    return df_filtered


def _filter_by_target_user_secret_id(
    df: pl.DataFrame, instrument_name: str
) -> pl.DataFrame:
    """
    Filter parent records using target_user_secret_id column.

    Parameters
    ----------
    df
        DataFrame with target_user_secret_id column
    instrument_name
        Name of the instrument being processed

    Returns
    -------
    pl.DataFrame
        Filtered DataFrame

    """
    # Check if this is curious_account_created instrument
    if instrument_name.startswith("curious_account_created"):
        return _filter_parent_records_for_account_created(df)
    return _filter_parent_records_from_instrument(df, instrument_name)


def _filter_by_record_id_fallback(
    df: pl.DataFrame, instrument_name: str
) -> pl.DataFrame:
    """
    Filter parent records using record_id column as fallback.

    Parameters
    ----------
    df
        DataFrame with record_id column
    instrument_name
        Name of the instrument being processed

    Returns
    -------
    pl.DataFrame
        Filtered DataFrame

    """
    logger.warning(
        "Instrument '%s' missing 'target_user_secret_id' column, "
        "attempting fallback filter on 'record_id'",
        instrument_name,
    )

    if "record_id" not in df.columns:
        logger.error(
            "Cannot filter parent records for '%s': "
            "missing both 'target_user_secret_id' and 'record_id' columns",
            instrument_name,
        )
        return df

    with_p_records = df.filter(pl.col("record_id").cast(pl.Utf8).str.ends_with("_P"))
    df_filtered = df.filter(~pl.col("record_id").cast(pl.Utf8).str.ends_with("_P"))

    if len(with_p_records) > 0:
        ignored_ids = with_p_records["record_id"].cast(pl.Utf8).to_list()
        logger.warning(
            "Filtered %d parent records by record_id in %s: %s",
            len(with_p_records),
            instrument_name,
            ", ".join(str(r) for r in ignored_ids),
        )

    return df_filtered


def _filter_parent_records(output: NamedOutput) -> NamedOutput:
    """
    Filter parent records (_P suffix) from output data.

    For curious_account_created: strip _P from record ID
    For other instruments: exclude records with _P suffix

    Parameters
    ----------
    output
        NamedOutput from formatter

    Returns
    -------
    NamedOutput
        Filtered output

    """
    df = output.output
    instrument_name = output.name

    # Try target_user_secret_id first
    if "target_user_secret_id" in df.columns:
        df_filtered = _filter_by_target_user_secret_id(df, instrument_name)
    else:
        # Fallback to record_id
        df_filtered = _filter_by_record_id_fallback(df, instrument_name)

    return NamedOutput(name=output.name, output=df_filtered)


def format_for_redcap(
    curious_data_dir: Path,
) -> tuple[list[NamedOutput], InstrumentRowCount]:
    """Format Curious data for REDCap."""
    event_names = get_redcap_event_names(
        ENDPOINTS["REDCap"].base_url, redcap_variables.headers, {"token": REDCAP_TOKEN}
    )

    # First pass: format without metadata to see what fields we have
    formatter_initial = RedcapImportFormat(project=event_names, redcap_metadata=None)

    # Process data
    try:
        ml_data = MindloggerData.create(curious_data_dir)
    except pl.exceptions.NoDataError as no_data_error:
        logger.info("No Curious data to export.")
        raise NoData from no_data_error

    # Get initial outputs to identify needed fields
    initial_outputs = formatter_initial.produce(ml_data)

    # Extract field names that need metadata
    field_names = _extract_field_names_from_outputs(initial_outputs)

    # Fetch metadata only for those specific fields
    redcap_metadata_pl = _fetch_redcap_metadata_for_fields(field_names)

    # Create formatter with metadata and re-process if we got metadata
    formatter = RedcapImportFormat(
        project=event_names, redcap_metadata=redcap_metadata_pl
    )
    if redcap_metadata_pl is not None:
        outputs = formatter.produce(ml_data)
    else:
        # Use initial outputs if no metadata was fetched
        outputs = initial_outputs

    # Filter parent records from all outputs
    filtered_outputs = [_filter_parent_records(output) for output in outputs]

    logger.info(
        "Data formatted for these instruments: %s",
        "".join([f"\n\t- {_.name[:-7]}" for _ in filtered_outputs]),
    )

    return filtered_outputs, formatter.get_instrument_row_counts()


def get_curious_data(request_json: CliOptions) -> None:
    """Try to pull Curious data."""
    tsx(
        Config.PROJECT_ROOT / "javascript_jobs/autoexport/src/index.ts",
        request_json.long.split(" "),
        parse_output=False,
    )


def _validate_csv_structure(
    df_polars: pl.DataFrame, csv_name: str
) -> tuple[bool, list[str]]:
    """
    Validate CSV structure and return data fields.

    Returns
    -------
    tuple[bool, list[str]]
        (is_valid, data_fields)

    """
    # Check if record_id column exists
    if "record_id" not in df_polars.columns:
        logger.debug("No record_id column in %s, skipping MRN validation", csv_name)
        return False, []
    # Get all data columns (excluding REDCap metadata columns)
    standard_fields = {
        "record_id",
        "redcap_event_name",
        "redcap_repeat_instrument",
        "redcap_repeat_instance",
    }
    data_fields = [col for col in df_polars.columns if col not in standard_fields]
    if not data_fields:
        logger.debug("No data fields found in %s", csv_name)
        return False, []
    return True, data_fields


def _prepare_mrn_lookup_data(
    df: pd.DataFrame, data_fields: list[str], csv_name: str
) -> tuple[pd.DataFrame | None, pd.DataFrame | None]:
    """
    Prepare data for MRN lookup.

    Returns
    -------
    tuple[pd.DataFrame | None, pd.DataFrame | None]
        (prepared_df, redcap_fields) or (None, None) if preparation fails

    """
    # Create a temporary DataFrame in the format expected by map_mrns_to_records
    temp_rows = []
    for _, row in df.head(10).iterrows():  # Sample first 10 rows for validation
        for field in data_fields[:5]:  # Sample first 5 fields
            temp_rows.append(
                {
                    "record": str(row["record_id"]),
                    "field_name": field,
                    "value": str(row[field]) if pd.notna(row[field]) else "",
                    "redcap_event_name": row.get("redcap_event_name", ""),
                }
            )
    if not temp_rows:
        return None, None
    prepared_df = pd.DataFrame(temp_rows)
    # Fetch existing REDCap data for these fields to get valid MRN mappings
    try:
        # Include 'mrn' field to ensure we get the MRN lookup
        fields_to_fetch = list({*data_fields[:10], "mrn"})
        redcap_fields = fetch_data(
            REDCAP_TOKEN, str(FieldList(fields_to_fetch)), all_or_any="any"
        )
        if redcap_fields.empty:
            logger.debug("No existing REDCap data found for fields in %s", csv_name)
            return None, None
        return prepared_df, redcap_fields
    except Exception as e:
        logger.warning("Could not fetch REDCap data for MRN validation: %s", e)
        return None, None


def _apply_mrn_mapping(
    df: pd.DataFrame,
    prepared_df: pd.DataFrame,
    redcap_fields: pd.DataFrame,
    csv_path: Path,
) -> bool:
    """
    Apply MRN mapping to the dataframe and save.

    Returns
    -------
    bool
        True if mapping was applied, False otherwise

    """
    try:
        _, mrn_lookup = map_mrns_to_records(prepared_df, redcap_fields)
        if not mrn_lookup:
            logger.debug("No MRN lookup data available for %s", csv_path.name)
            return False
        # Check if any MRNs need to be mapped
        original_records = set(df["record_id"].astype(str).unique())
        mappable_mrns = original_records & set(mrn_lookup.keys())
        if not mappable_mrns:
            logger.debug("No MRNs found that need mapping in %s", csv_path.name)
            return False
        # Apply the mapping
        df["record_id"] = (
            df["record_id"].astype(str).map(lambda x: mrn_lookup.get(str(x), x))
        )
        # Convert back to polars and save
        df_polars_updated = pl.from_pandas(df)
        df_polars_updated.write_csv(csv_path)
        logger.info(
            "Mapped %d MRNs to record IDs in %s (out of %d total records)",
            len(mappable_mrns),
            csv_path.name,
            len(original_records),
        )
        return True
    except Exception as e:
        logger.warning("Error during MRN mapping for %s: %s", csv_path.name, e)
        return False


def validate_and_map_mrns(csv_path: Path) -> bool:
    """
    Validate and map MRNs to record IDs if needed.

    This function checks if the CSV contains MRN data that needs to be
    mapped to record IDs, and performs the mapping using the shared
    map_mrns_to_records utility.

    Parameters
    ----------
    csv_path
        Path to CSV file to validate and potentially update

    Returns
    -------
    bool
        True if MRN mapping was applied, False otherwise

    """
    # Read the CSV with polars first to check structure
    df_polars = pl.read_csv(csv_path)
    # Validate CSV structure
    is_valid, data_fields = _validate_csv_structure(df_polars, csv_path.name)
    if not is_valid:
        return False
    logger.info("Validating MRN mapping for %s", csv_path.name)
    # Convert to pandas for compatibility with map_mrns_to_records
    df = df_polars.to_pandas()
    # Prepare lookup data
    prepared_df, redcap_fields = _prepare_mrn_lookup_data(
        df, data_fields, csv_path.name
    )
    if prepared_df is None or redcap_fields is None:
        return False
    # Apply MRN mapping
    return _apply_mrn_mapping(df, prepared_df, redcap_fields, csv_path)


def get_redcap_records_for_instrument(
    instrument: str, records: list[str], event: str = ""
) -> dict[str, dict[str, str]]:
    """
    Get existing REDCap data for specific records and instrument.

    Returns
    -------
    dict
        Dict mapping record_id to field values for that record

    """
    # Get all fields for this instrument from metadata
    metadata = fetch_api_data(
        ENDPOINTS["REDCap"].base_url,
        redcap_variables.headers,
        {
            "token": REDCAP_TOKEN,
            "content": "metadata",
            "format": "json",
            "forms": instrument,
        },
    )
    # Extract field names - metadata is a DataFrame
    if metadata.empty:
        logger.warning("No metadata found for instrument: %s", instrument)
        return {}
    fields = metadata["field_name"].tolist() if "field_name" in metadata.columns else []
    alert_field = f"{instrument}_alerts"
    # Only include alert field if it exists
    if alert_field not in fields:
        logger.debug("Alert field %s not found in instrument metadata", alert_field)
        return {}
    params = {
        "token": REDCAP_TOKEN,
        "content": "record",
        "format": "json",
        "type": "flat",
        "records": ",".join(records),
        "fields": f"record_id,{alert_field}",
    }
    if event:
        params["events"] = event
    try:
        response = requests.post(
            ENDPOINTS["REDCap"].base_url, data=params, headers=redcap_variables.headers
        )
        response.raise_for_status()
        data = response.json()
        # Convert to dict keyed by record_id
        result = {}
        for record in data:
            record_id = record.get("record_id", "")
            if record_id:
                result[record_id] = record
        return result
    except Exception as e:
        logger.warning("Could not fetch REDCap data for %s: %s", instrument, e)
        return {}


def _determine_record_id_column(df: pl.DataFrame) -> str | None:
    """Find the record ID column in the DataFrame."""
    for possible_col in ["record_id", "record", "participant_id", "subject_id"]:
        if possible_col in df.columns:
            return possible_col
    return None


def _determine_event_column(df: pl.DataFrame) -> str | None:
    """Find the event column in the DataFrame."""
    for possible_col in ["redcap_event_name", "event"]:
        if possible_col in df.columns:
            return possible_col
    return None


def add_alert_fields_if_needed(csv_path: Path) -> None:
    """
    Add alert fields with 'no' value if they don't already exist as 'yes'.

    This function:
    1. Checks if instrument can have alerts
    2. Waits 15 seconds for any real-time websocket alerts to arrive
    3. Checks REDCap for existing alert status
    4. Only sets alert='no' if it's not already 'yes'

    This prevents race conditions with alerts_to_redcap.py which uses websockets
    to set alerts='yes' in real-time.
    """
    # Read the CSV
    df = pl.read_csv(csv_path)
    instrument_name = csv_path.stem.lower()
    # Check if this instrument can have alerts
    alert_instruments = [
        inst.lower()
        for inst in possible_alert_instruments(ENDPOINTS["REDCap"].base_url)
    ]
    if instrument_name not in alert_instruments:
        logger.debug("Instrument %s does not have alerts", instrument_name)
        return
    logger.info("Processing alerts for instrument: %s", instrument_name)
    # Determine record ID column
    record_id_col = _determine_record_id_column(df)
    if not record_id_col:
        logger.warning("Could not find record ID column in %s", instrument_name)
        return
    # Determine event column (if present in the data)
    event_col = _determine_event_column(df)
    alert_field = f"{instrument_name}_alerts"
    # Check if alert field already exists in the data
    if alert_field in df.columns:
        logger.info("Alert field %s already exists in data", alert_field)
        return
    # Get unique records from this batch
    records_str = [str(r) for r in df[record_id_col].unique().to_list()]
    # Wait 15 seconds to allow websocket alerts to arrive first
    logger.info(
        "Waiting 15 seconds before checking REDCap alert status for %d records...",
        len(records_str),
    )
    time.sleep(15)
    # Determine the event name for the ALERT FIELD (not from the data event)
    # Alert fields are on ra_alerts_parent/child forms, typically on admin_arm_1
    alert_event = get_alert_field_event(
        ENDPOINTS["REDCap"].base_url,
        instrument_name,
    )
    if not alert_event:
        logger.warning(
            "Could not determine event for alert field %s, skipping alert processing",
            alert_field,
        )
        return
    # Fetch existing REDCap data for all records
    existing_data = get_redcap_records_for_instrument(
        instrument_name, records_str, alert_event
    )
    # Build list of alert rows to add
    alert_rows = []
    records_with_alerts = 0
    records_setting_no = 0
    for record_id in df[record_id_col].unique():
        record_id_str = str(record_id)
        # Check if this record already has an alert set to "yes"
        existing_record = existing_data.get(record_id_str, {})
        existing_status = existing_record.get(alert_field, "").strip()
        if existing_status == "yes":
            # Don't add a row - REDCap already has "yes" for this record
            records_with_alerts += 1
            logger.debug(
                "Preserving %s='yes' for record %s", alert_field, record_id_str
            )
        else:
            # Create a new row for the alert field with the correct event
            alert_row = {record_id_col: record_id}
            # Add only the alert field and event
            alert_row[alert_field] = "no"
            if event_col:
                alert_row[event_col] = alert_event
            alert_rows.append(alert_row)
            records_setting_no += 1
    logger.info(
        "Alert field summary for %s: %d records already have alerts='yes', "
        "%d records will be set to 'no'",
        instrument_name,
        records_with_alerts,
        records_setting_no,
    )
    if not alert_rows:
        logger.info("No alert rows to add for %s", instrument_name)
        return
    # Create a DataFrame for the alert rows
    alert_df = pl.DataFrame(alert_rows)
    # If there's no event column in the original data, add it
    if not event_col:
        alert_df = alert_df.with_columns(pl.lit(alert_event).alias("redcap_event_name"))
    # Append alert rows to the original DataFrame
    df = pl.concat([df, alert_df], how="diagonal")
    logger.info(
        "Added %d alert rows for instrument %s with event %s",
        len(alert_rows),
        instrument_name,
        alert_event,
    )
    # Write back to CSV
    df.write_csv(csv_path)
    logger.info("Updated %s with alert field", csv_path)


def extract_unfound_fields(error_text: str) -> list[str]:
    """
    Extract field names from REDCap error message.

    Parameters
    ----------
    error_text : str
        The error response text from REDCap

    Returns
    -------
    list[str]
        List of field names that were not found

    """
    match = re.search(
        r"The following fields were not found in the project as real data fields:"
        r" (.+?)(?:\n|$)",
        error_text,
    )
    if match:
        fields_str = match.group(1)
        # Split by comma and strip whitespace
        return [field.strip() for field in fields_str.split(",")]
    return []


def extract_invalid_category_errors(error_text: str) -> list[dict[str, str]]:
    """
    Extract invalid category errors from REDCap error message.

    Parameters
    ----------
    error_text : str
        The error response text from REDCap

    Returns
    -------
    list[dict[str, str]]
        List of dicts with keys: record, field_name, value, error_message

    """
    errors = []
    pattern = (
        r'"([^"]+)","([^"]+)","([^"]*)","The value is not a valid category for ([^"]+)"'
    )
    for match in re.finditer(pattern, error_text):
        record_id, field_name, value, _ = match.groups()
        errors.append(
            {
                "record": record_id,
                "field_name": field_name,
                "value": value,
                "error_message": f"The value is not a valid category for {field_name}",
            }
        )
    return errors


def save_invalid_category_errors(
    csv_path: Path,
    errors: list[dict[str, str]],
) -> Path:
    """
    Save invalid category errors to log file.

    Parameters
    ----------
    csv_path : Path
        Original CSV file path (for naming the log file)
    errors : list[dict[str, str]]
        List of error dicts from extract_invalid_category_errors

    Returns
    -------
    Path
        Path to the saved error log file

    """
    # Create invalid_categories subdirectory in LOG_ROOT
    log_dir = Config.LOG_ROOT / "invalid_categories"
    log_dir.mkdir(parents=True, exist_ok=True)
    # Add timestamp to filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    error_log_path = log_dir / f"{csv_path.stem}_invalid_categories_{timestamp}.csv"
    # Write errors to CSV
    with error_log_path.open("w", newline="") as f:
        if errors:
            writer = csv.DictWriter(
                f, fieldnames=["record", "field_name", "value", "error_message"]
            )
            writer.writeheader()
            writer.writerows(errors)
    return error_log_path


def split_csv_by_fields(
    csv_path: Path, unfound_fields: list[str]
) -> tuple[Path, Path | None]:
    """
    Split CSV into two files: one with unfound fields, one without.

    Parameters
    ----------
    csv_path : Path
        Path to the original CSV file
    unfound_fields : list[str]
        List of field names that were not found in REDCap

    Returns
    -------
    tuple[Path, Path | None]
        (path_to_valid_fields_csv, path_to_unfound_fields_csv)
        The unfound_fields_csv will be saved under Config.LOG_ROOT

    """
    df = pl.read_csv(csv_path)
    # Identify record identifier columns to keep in unfound file
    identifier_cols = []
    for col in [
        "record_id",
        "redcap_event_name",
        "redcap_repeat_instrument",
        "redcap_repeat_instance",
    ]:
        if col in df.columns:
            identifier_cols.append(col)
    # Columns that exist in the dataframe and are in unfound list
    unfound_cols_present = [col for col in unfound_fields if col in df.columns]
    if not unfound_cols_present:
        logger.info("None of the unfound fields are present in the CSV")
        return csv_path, None
    # Create unfound fields dataframe (identifiers + unfound columns)
    unfound_df = df.select(identifier_cols + unfound_cols_present)
    # Create valid fields dataframe (all columns except unfound)
    valid_cols = [col for col in df.columns if col not in unfound_cols_present]
    valid_df = df.select(valid_cols)
    # Save valid fields to temp location (will be used immediately)
    valid_path = csv_path.with_stem(f"{csv_path.stem}_valid_fields")
    valid_df.write_csv(valid_path)
    log_dir = Config.LOG_ROOT / "unfound_fields"
    log_dir.mkdir(parents=True, exist_ok=True)
    # Add timestamp to filename for uniqueness
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    unfound_path = log_dir / f"{csv_path.stem}_unfound_fields_{timestamp}.csv"
    unfound_df.write_csv(unfound_path)
    logger.info(
        "Split %s into:\n\t- Valid fields (%d columns): %s\n"
        "\t- Unfound fields (%d columns): %s",
        csv_path.name,
        len(valid_cols),
        valid_path.name,
        len(unfound_cols_present),
        unfound_path,
    )
    return valid_path, unfound_path


def validate_fields_against_metadata(
    df: pd.DataFrame, metadata: pd.DataFrame, instrument_name: str
) -> tuple[list[str], list[str]]:
    """
    Validate dataframe columns against REDCap metadata.

    Returns:
        tuple: (valid_fields, invalid_fields)

    """
    # Get all valid field names from metadata
    valid_fields: set[str] = {
        *metadata["field_name"].tolist(),
        f"{instrument_name.lower()}_complete",
    }

    # Get fields from dataframe (excluding record_id and redcap_event_name)
    df_fields = set(df.columns) - {"record_id", "redcap_event_name"}

    # Find invalid fields
    invalid_fields = [f for f in df_fields if f not in valid_fields]
    valid_df_fields = [f for f in df_fields if f in valid_fields]

    if invalid_fields:
        logger.warning(
            "Found %d invalid fields in %s before upload: %s",
            len(invalid_fields),
            instrument_name,
            ", ".join(invalid_fields),
        )

    return valid_df_fields, invalid_fields


def chunk_dataframe_by_columns(
    df: pl.DataFrame,
    max_columns: int = 100,
    required_columns: list[str] | None = None,
) -> list[pl.DataFrame]:
    """
    Split a DataFrame into chunks by columns to avoid API timeouts.

    Parameters
    ----------
    df
        DataFrame to chunk
    max_columns
        Maximum number of data columns per chunk (excluding required columns)
    required_columns
        Columns that must be in every chunk (e.g., record_id, event)

    Returns
    -------
    list[pl.DataFrame]
        List of DataFrames, each with required columns plus a subset of data columns

    """
    if required_columns is None:
        required_columns = ["record_id", "redcap_event_name"]

    # Identify required vs data columns
    required_cols = [col for col in required_columns if col in df.columns]
    data_cols = [col for col in df.columns if col not in required_cols]

    # If total columns is manageable, return as-is
    if len(df.columns) <= max_columns + len(required_cols):
        return [df]

    # Chunk the data columns
    chunks = []
    for i in range(0, len(data_cols), max_columns):
        chunk_data_cols = data_cols[i : i + max_columns]
        chunk_df = df.select(required_cols + chunk_data_cols)
        chunks.append(chunk_df)
        logger.debug(
            "Created chunk %d with %d columns (%d data + %d required)",
            len(chunks),
            len(chunk_df.columns),
            len(chunk_data_cols),
            len(required_cols),
        )

    return chunks


def _upload_csv_to_redcap(csv_path: Path, retry_on_field_error: bool = True) -> None:
    """
    Upload a single CSV file to REDCap.

    Parameters
    ----------
    csv_path
        Path to CSV file to upload
    retry_on_field_error
        If True, will retry with only valid fields if field error occurs

    """
    with csv_path.open("r") as csv_content:
        data = {
            "token": REDCAP_TOKEN,
            "content": "record",
            "action": "import",
            "format": "csv",
            "type": "flat",
            "overwriteBehavior": "normal",
            "forceAutoNumber": "false",
            "data": csv_content.read(),
            "returnContent": "count",
            "returnFormat": "csv",
        }
        r = requests.post(
            ENDPOINTS["REDCap"].base_url, data=data, timeout=180
        )  # 3 minute timeout
        if r.status_code != requests.codes["okay"]:
            logger.error("Bad Request")
            logger.error(r.text)
            logger.error("HTTP Status: %d", r.status_code)
            # Check for invalid category errors
            invalid_cat_errors = extract_invalid_category_errors(r.text)
            if invalid_cat_errors:
                error_log_path = save_invalid_category_errors(
                    csv_path, invalid_cat_errors
                )
                logger.warning(
                    "Found %d invalid category errors. Saved to: %s",
                    len(invalid_cat_errors),
                    error_log_path,
                )
                # Still raise the error after logging
                r.raise_for_status()
            # Check if this is a field not found error
            if (
                retry_on_field_error
                and "fields were not found in the project" in r.text
            ):
                unfound_fields = extract_unfound_fields(r.text)
                if unfound_fields:
                    logger.warning(
                        "Found %d unfound fields: %s",
                        len(unfound_fields),
                        ", ".join(unfound_fields),
                    )
                    # Split the CSV
                    valid_path, unfound_path = split_csv_by_fields(
                        csv_path, unfound_fields
                    )
                    # Log the unfound fields data location
                    if unfound_path:
                        logger.warning("Unfound fields data saved to: %s", unfound_path)
                    # Retry with valid fields only
                    if valid_path != csv_path:
                        logger.info("Retrying upload with only valid fields...")
                        _upload_csv_to_redcap(valid_path, retry_on_field_error=False)
                        return
            # If we get here, either it's not a field error or retry failed
            r.raise_for_status()


def push_to_redcap(
    csv_path: Path,
    retry_on_field_error: bool = True,
    skip_deduplication: bool = False,
) -> None:
    """
    Push data to REDCap with preprocessing, validation, and deduplication.

    Parameters
    ----------
    csv_path : Path
        Path to CSV file to upload
    retry_on_field_error : bool
        If True, will retry with only valid fields if field error occurs
    skip_deduplication : bool
        If True, skip deduplication (for testing)

    """
    if not csv_path.stat().st_size:
        logger.info("Skipping empty file: %s", csv_path)
        return

    # Preprocessing
    validate_and_map_mrns(csv_path)
    add_alert_fields_if_needed(csv_path)

    # Load and validate
    df = pl.read_csv(csv_path)
    required_cols = [col for col in STANDARD_FIELDS if col in df.columns]

    # Pre-upload validation
    instrument_name = csv_path.stem
    try:
        metadata = fetch_api_data(
            ENDPOINTS["REDCap"].base_url,
            redcap_variables.headers,
            {"token": REDCAP_TOKEN, "content": "metadata", "format": "csv"},
        )

        if not metadata.empty:
            df_pandas = df.to_pandas()
            valid_fields, invalid_fields = validate_fields_against_metadata(
                df_pandas, metadata, instrument_name
            )

            if invalid_fields:
                unfound_path = split_csv_by_fields(csv_path, invalid_fields)[1]
                if unfound_path:
                    logger.warning(
                        "Pre-upload: saved %d unfound fields to %s",
                        len(invalid_fields),
                        unfound_path,
                    )

                valid_cols = required_cols + [
                    f
                    for f in valid_fields
                    if f in df.columns and f not in required_cols
                ]
                df = df.select(valid_cols)
                logger.info(
                    "Proceeding with %d valid columns (removed %d invalid)",
                    len(df.columns),
                    len(invalid_fields),
                )
    except Exception as e:
        logger.warning("Could not perform pre-upload validation: %s", e)

    # Deduplication (can be skipped for testing)
    if not skip_deduplication:
        df, _num_duplicates = deduplicate_dataframe(
            df,
            REDCAP_TOKEN,
            ENDPOINTS["REDCap"].base_url,
            redcap_variables.headers,
            instrument_name,
        )

        if df.is_empty():
            logger.info("All rows in %s are duplicates, skipping upload", csv_path.name)
            return

    # Upload (chunked or single)
    if len(df.columns) > Config.COLUMN_CHUNK_SIZE:
        logger.info("Large dataset (%d columns), chunking...", len(df.columns))
        chunks = chunk_dataframe_by_columns(
            df, max_columns=100, required_columns=required_cols
        )
        logger.info("Split into %d chunks", len(chunks))

        for i, chunk in enumerate(chunks, 1):
            chunk_path = csv_path.with_stem(f"{csv_path.stem}_chunk_{i}")
            chunk.write_csv(chunk_path)
            logger.info(
                "Uploading chunk %d/%d (%d columns)...",
                i,
                len(chunks),
                len(chunk.columns),
            )

            try:
                _upload_csv_to_redcap(chunk_path, retry_on_field_error)
                logger.info("Successfully uploaded chunk %d/%d", i, len(chunks))
            finally:
                if chunk_path.exists():
                    chunk_path.unlink()
    else:
        # Write df back to CSV path before upload
        df.write_csv(csv_path)
        _upload_csv_to_redcap(csv_path, retry_on_field_error)


def save_for_redcap(outputs: list[NamedOutput], redcap_data_dir: Path):
    """Save REDCap data."""
    # Save outputs
    for output in outputs:
        nested_cols = [
            col
            for col in output.output.columns
            if output.output[col].dtype in [pl.List, pl.Struct]
            or str(output.output[col].dtype).startswith("List")
            or str(output.output[col].dtype).startswith("Struct")
        ]
        if nested_cols:
            logger.info("Output '%s' has nested columns: %s", output.name, nested_cols)
            for col in nested_cols:
                logger.info("  %s: %s", col, output.output[col].dtype)
        output.output.write_csv(
            (redcap_data_dir / output.name.replace("_redcap", "")).with_suffix(".csv")
        )


def send_to_redcap(
    redcap_path: Path,
    instrument_row_count: dict[str, int],
    cache: DataCache | None = None,
) -> Results:
    """Send data to REDCap."""
    results = Results()
    instruments: list[str] = [
        instrument.lower()
        for instrument in list(
            fetch_api_data(
                ENDPOINTS["REDCap"].base_url,
                redcap_variables.headers,
                {
                    "token": REDCAP_TOKEN,
                    "content": "instrument",
                    "format": "csv",
                    "returnFormat": "csv",
                },
            )["instrument_name"]
        )
    ]
    to_send = [
        instrument
        for instrument in redcap_path.iterdir()
        if instrument.stem.lower() in list(instruments)
    ]

    logger.info(
        "Ready to send to REDCap: %s",
        "".join([f"\n\t- {file.stem}" for file in to_send]),
    )
    for instrument in to_send:
        instrument_key = instrument.stem
        # Generate cache key based on file content hash (not just instrument name)
        # This ensures we only skip if the exact same data was already sent
        cache_key = instrument_key
        file_hash: str = ""
        if cache:
            # Read file and create hash of content
            file_hash = hashlib.md5(instrument.read_bytes()).hexdigest()
            cache_key = f"{instrument_key}_{file_hash}"
            # Check cache to skip if this exact data was already processed
            if cache.is_processed(cache_key):
                logger.info(
                    "Skipping %s (exact data already processed in cache)",
                    instrument_key,
                )
                results.success += instrument_row_count.get(instrument.stem, 0)
                continue
        try:
            push_to_redcap(instrument)
            results.success += instrument_row_count.get(instrument.stem, 0)
            # Mark this specific data as processed in cache
            if cache:
                cache.mark_processed(
                    cache_key,
                    metadata={
                        "row_count": instrument_row_count.get(instrument.stem, 0),
                        "file_hash": file_hash,
                    },
                )
        except Exception:
            logger.exception("%s\n", instrument)
            results.failure.append(instrument.stem)
    return results


def data_to_redcap(
    applet_name: str, request_json: CliOptions, cache: DataCache
) -> None:
    """Send Curious data to REDCap for `applet_name`."""
    with TemporaryDirectory() as curious_temp_data_dir:
        applet_credentials = APPLET_CREDENTIALS[applet_name]
        os.environ.update(
            {key.upper(): value for key, value in applet_credentials.items()}
        )
        logger.info(
            "%s", {key.upper(): value for key, value in applet_credentials.items()}
        )
        root_temp_path = Path(curious_temp_data_dir)
        data_dir_paths = {
            source: root_temp_path / f"from_{source}"
            for source in ["curious", "redcap"]
        }
        for path in data_dir_paths.values():
            path.mkdir(parents=True, exist_ok=True)
        curious_export_file = data_dir_paths["curious"] / "responses_curious.csv"
        request_json["output"] = str(curious_export_file)
        get_curious_data(request_json)

        # format_for_redcap now handles the REDCap metadata mapping internally
        try:
            outputs, _instrument_row_count = format_for_redcap(
                data_dir_paths["curious"]
            )
        except NoData:
            return

        instrument_row_count: dict[str, int] = {
            k: v for k, v in _instrument_row_count.items() if v is not None
        }
        save_for_redcap(outputs, data_dir_paths["redcap"])
        results = send_to_redcap(data_dir_paths["redcap"], instrument_row_count, cache)
        # Log cache statistics
        cache_stats = cache.get_stats()
        logger.info(
            "Cache statistics: %d entries, file size: %d bytes, last activity: %s",
            cache_stats["total_entries"],
            cache_stats["file_size_bytes"],
            cache_stats.get("last_activity", "never"),
        )
        logger.info(results.report, YESTERDAY)


def main() -> None:
    """Send Curious data to REDCap."""
    # Use 2-minute window for minute-by-minute transfers
    # Auto-fallback to full-day on downtime (2+ hours without activity)
    # Recovery mode is logged by get_recent_time_window()
    from_date, to_date = get_recent_time_window(
        minutes_back=2, allow_full_day_fallback=True
    )
    request_json = CliOptions({"fromDate": from_date, "toDate": to_date})
    # Initialize cache for minute-by-minute transfers (TTL: 2 minutes)
    cache = DataCache("curious_data_to_redcap2", ttl_minutes=2)
    exceptions = False
    for project in curious_variables.applets.keys():
        try:
            logger.info(
                "\n=====\nTransferring %s from Curious to REDCap.\n=====\n", project
            )
            data_to_redcap(project, request_json, cache)
        except NoData:
            pass
        except Exception:
            logger.exception(
                "Failed to transfer %s data from Curious to REDCap.", project
            )
            exceptions = True
    sys.exit(bool(exceptions))


if __name__ == "__main__":
    main()
