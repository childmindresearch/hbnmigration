"""Send Curious data to REDCap."""

from datetime import datetime
import logging
import os
from pathlib import Path
import re
import sys
from tempfile import TemporaryDirectory
import time
from typing import Literal

import polars as pl
import requests

from mindlogger_data_export.mindlogger import MindloggerData
from mindlogger_data_export.outputs import NamedOutput, RedcapImportFormat

from .._config_variables import curious_variables, redcap_variables
from ..config import Config
from ..utility_functions import (
    CliOptions,
    Endpoints,
    fetch_api_data,
    get_redcap_event_names,
    initialize_logging,
    InstrumentRowCount,
    Results,
    today,
    tsx,
    yesterday,
)
from .utils import (
    get_alert_field_event,
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
    logger.info(
        "Data formatted for these instruments: %s",
        "".join([f"\n\t- {_.name[:-7]}" for _ in outputs]),
    )
    return outputs, formatter.get_instrument_row_counts()


def get_curious_data(request_json: CliOptions) -> None:
    """Try to pull Curious data."""
    tsx(
        Config.PROJECT_ROOT / "javascript_jobs/autoexport/src/index.ts",
        request_json.long.split(" "),
        parse_output=False,
    )


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


def _determine_event_for_alert(
    df: pl.DataFrame,
    event_col: str | None,
    instrument_name: str,
    base_url: str,
) -> str | None:
    """Determine the REDCap event name for this instrument's alert field."""
    if event_col and len(df) > 0:
        # First try: use the event from the CSV data
        return str(df[event_col][0])
    return get_alert_field_event(base_url, instrument_name)


def _build_alert_values(  # noqa: PLR0913
    df: pl.DataFrame,
    record_id_col: str,
    event_col: str | None,
    event: str | None,
    alert_field: str,
    existing_data: dict[str, dict[str, str]],
) -> tuple[list[str], list[str | None], int, int]:
    """
    Build alert values based on existing REDCap data.

    Returns
    -------
    tuple
        (alert_values, event_values, records_with_alerts, records_setting_no)

    """
    alert_values = []
    event_values = []
    records_with_alerts = 0
    records_setting_no = 0
    for row in df.iter_rows(named=True):
        record_id = str(row[record_id_col])
        row_event = str(row[event_col]) if event_col else event
        # Check if this record already has an alert set to "yes"
        existing_record = existing_data.get(record_id, {})
        existing_status = existing_record.get(alert_field, "").strip()
        if existing_status == "yes":
            # don't overwrite existing "yes"
            # leave blank so REDCap keeps existing value
            alert_values.append("")
            records_with_alerts += 1
            logger.debug("Preserving %s='yes' for record %s", alert_field, record_id)
        else:
            # Set to "no" since it's either empty or something other than "yes"
            alert_values.append("no")
            records_setting_no += 1
        event_values.append(row_event)
    return alert_values, event_values, records_with_alerts, records_setting_no


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
        r"The following fields were not found in the project as real data fields: "
        r"(.+?)(?:\n|$)",
        error_text,
    )
    if match:
        fields_str = match.group(1)
        # Split by comma and strip whitespace
        return [field.strip() for field in fields_str.split(",")]
    return []


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

    # Save unfound fields to LOG_ROOT for permanent storage
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


def send_to_redcap(redcap_path: Path, instrument_row_count: dict[str, int]) -> Results:
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
        try:
            push_to_redcap(instrument)
            results.success += instrument_row_count.get(instrument.stem, 0)
        except Exception:
            logger.exception("%s\n", instrument)
            results.failure.append(instrument.stem)
    return results


def main() -> None:
    """Send Curious data to REDCap."""
    request_json = CliOptions({"fromDate": yesterday, "toDate": today})
    """All data from yesterday to now."""
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
        results = send_to_redcap(data_dir_paths["redcap"], instrument_row_count)
        logger.info(results.report, yesterday)


if __name__ == "__main__":
    main()
