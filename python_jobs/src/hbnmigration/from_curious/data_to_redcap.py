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
    get_alert_field_event,
    map_mrns_to_records,
    possible_alert_instruments,
    REDCAP_TOKEN,
)

initialize_logging()
logger = logging.getLogger(__name__)

ENDPOINTS: dict[Literal["Curious", "REDCap"], Endpoints] = {
    "Curious": curious_variables.Endpoints(),
    "REDCap": redcap_variables.Endpoints(),
}
"""Initialized endpoints"""


def format_for_redcap(
    curious_data_dir: Path,
) -> tuple[list[NamedOutput], InstrumentRowCount]:
    """Format Curious data for REDCap."""
    event_names = get_redcap_event_names(
        ENDPOINTS["REDCap"].base_url, redcap_variables.headers, {"token": REDCAP_TOKEN}
    )
    # Create formatter with project name
    formatter = RedcapImportFormat(project=event_names)
    # Process data
    try:
        ml_data = MindloggerData.create(curious_data_dir)
    except pl.exceptions.NoDataError:
        logger.info("No Curious data to export.")
        sys.exit(0)
    outputs = formatter.produce(ml_data)
    # Process records where target subject is a parent (_P suffix)
    # For curious_account_created: strip _P from record ID
    # For other instruments: ignore records with _P suffix
    filtered_outputs = []
    for output in outputs:
        df = output.output
        if "target_user_secret_id" in df.columns:
            instrument_name = output.name
            # Check if this is curious_account_created instrument
            if instrument_name.startswith("curious_account_created"):
                # For curious_account_created: strip _P from record ID
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
                        "Stripped '_P' suffix from %d records in %s",
                        len(with_p),
                        instrument_name,
                    )
                    df_filtered = pl.concat([without_p, with_p])
                else:
                    df_filtered = df
            else:
                # For other instruments: filter out rows with _P suffix
                with_p = df.filter(
                    pl.col("target_user_secret_id").cast(pl.Utf8).str.ends_with("_P")
                )
                df_filtered = df.filter(
                    ~pl.col("target_user_secret_id").cast(pl.Utf8).str.ends_with("_P")
                )
                if len(with_p) > 0:
                    ignored_records = (
                        with_p["target_user_secret_id"].cast(pl.Utf8).to_list()
                    )
                    logger.info(
                        "Ignored %d records with '_P' suffix in %s: %s",
                        len(with_p),
                        instrument_name,
                        ", ".join(str(r) for r in ignored_records),
                    )
            # Create new NamedOutput with processed data
            filtered_outputs.append(NamedOutput(name=output.name, output=df_filtered))
        else:
            # No target_user_secret_id column - check record_id for _P suffix fallback
            instrument_name = output.name
            logger.warning(
                "Instrument '%s' missing 'target_user_secret_id' column, "
                "attempting fallback filter on 'record_id'",
                instrument_name,
            )
            # Try to filter by record_id column if it exists
            if "record_id" in df.columns:
                with_p_records = df.filter(
                    pl.col("record_id").cast(pl.Utf8).str.ends_with("_P")
                )
                df_filtered = df.filter(
                    ~pl.col("record_id").cast(pl.Utf8).str.ends_with("_P")
                )
                if len(with_p_records) > 0:
                    ignored_ids = with_p_records["record_id"].cast(pl.Utf8).to_list()
                    logger.warning(
                        "Filtered %d parent records by record_id in %s: %s",
                        len(with_p_records),
                        instrument_name,
                        ", ".join(str(r) for r in ignored_ids),
                    )
                filtered_outputs.append(
                    NamedOutput(name=output.name, output=df_filtered)
                )
            else:
                # No suitable column found for _P filtering
                logger.error(
                    "Cannot filter parent records for '%s': "
                    "missing both 'target_user_secret_id' and 'record_id' columns",
                    instrument_name,
                )
                filtered_outputs.append(output)
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


def push_to_redcap(csv_path: Path, retry_on_field_error: bool = True) -> None:
    """
    Push data to RedCap.

    Parameters
    ----------
    csv_path : Path
        Path to CSV file to upload
    retry_on_field_error : bool
        If True, will retry with only valid fields if field error occurs

    """
    if not csv_path.stat().st_size:
        logger.info("Skipping empty file: %s", csv_path)
        return

    # Validate and map MRNs if needed before adding alert fields
    validate_and_map_mrns(csv_path)

    # Add alert fields if needed before pushing
    add_alert_fields_if_needed(csv_path)

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
        r = requests.post(ENDPOINTS["REDCap"].base_url, data=data)
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
                        push_to_redcap(valid_path, retry_on_field_error=False)
                        return
            # If we get here, either it's not a field error or retry failed
            r.raise_for_status()


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


def main() -> None:
    """Send Curious data to REDCap."""
    # Use 2-minute window for minute-by-minute transfers
    # Auto-fallback to full-day on downtime (2+ hours without activity)
    # Recovery mode is logged by get_recent_time_window()
    from_date, to_date = get_recent_time_window(
        minutes_back=2, allow_full_day_fallback=True
    )
    # Convert ISO datetime to date-only format for TypeScript (YYYY-MM-DD)
    # TypeScript's setDateParams will append T00:00:00 and T23:59:59
    from_date_only = from_date.split("T")[0]  # Extract YYYY-MM-DD
    to_date_only = to_date.split("T")[0]  # Extract YYYY-MM-DD
    request_json = CliOptions({"fromDate": from_date_only, "toDate": to_date_only})
    # Initialize cache for minute-by-minute transfers (TTL: 2 minutes)
    cache = DataCache("curious_data_to_redcap", ttl_minutes=2)
    with TemporaryDirectory() as curious_temp_data_dir:
        applet_credentials = curious_variables.AppletCredentials.hbn_mindlogger[
            "Healthy Brain Network Questionnaires"
        ]
        os.environ.update(
            {key.upper(): value for key, value in applet_credentials.items()}
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
        outputs, _instrument_row_count = format_for_redcap(data_dir_paths["curious"])
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


if __name__ == "__main__":
    main()
