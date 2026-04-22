"""Transfer data from Ripple to REDCap."""

from datetime import datetime
import hashlib
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

from .._config_variables import redcap_variables, ripple_variables
from ..config import Config
from ..exceptions import NoData
from ..utility_functions import (
    create_composite_cache_key,
    DataCache,
    fetch_api_data,
    get_recent_time_window,
    initialize_logging,
    log_cache_statistics,
    ProjectStatus,
)

logger = initialize_logging(__name__)


class Endpoints:
    """API Endpoints."""

    REDCap = redcap_variables.Endpoints()
    """REDCap API endpoints."""
    Ripple = ripple_variables.Endpoints()
    """Ripple API endpoints."""


# ============================================================================
# Cache Key Utilities
# ============================================================================


def create_ripple_record_cache_key(mrn: str, email: str, last_modified: str) -> str:
    """
    Create a unique cache key for Ripple record.

    Parameters
    ----------
    mrn : str
        Medical record number
    email : str
        Contact email
    last_modified : str
        Last modification timestamp from Ripple

    Returns
    -------
    str
        Composite cache key like "12345:abc456:2024-01-15"

    """
    # Use only date portion of timestamp for daily granularity
    date_part = (
        last_modified.split("T", maxsplit=1)[0]
        if "T" in last_modified
        else last_modified
    )
    # Hash email for privacy
    email_hash = hashlib.md5(email.encode()).hexdigest()[:8]
    return create_composite_cache_key(mrn, email_hash, date_part)


def extract_last_modified(ripple_df: pd.DataFrame) -> pd.Series:
    """
    Extract last modified timestamp from Ripple data.

    Parameters
    ----------
    ripple_df : pd.DataFrame
        Ripple DataFrame

    Returns
    -------
    pd.Series
        Series with last modified timestamps

    """
    # Look for common timestamp columns
    for col in ["lastModified", "updatedAt", "modifiedDate", "updated_at"]:
        if col in ripple_df.columns:
            return ripple_df[col]

    # If no timestamp column, use current time
    return pd.Series(
        [datetime.now().isoformat()] * len(ripple_df), index=ripple_df.index
    )


# ============================================================================
# Data Fetching
# ============================================================================


def request_potential_participants() -> pd.DataFrame:
    """Request Ripple potential participants data via Ripple API Export."""
    # Use 2-minute window for minute-by-minute transfers
    from_date, _ = get_recent_time_window(minutes_back=2)

    ripple_df = pd.concat(
        [
            Endpoints.Ripple.export_from_ripple(
                ripple_study,
                {
                    "surveyExportSince": from_date,
                    **ripple_variables.column_dict(
                        [
                            "globalId",
                            "customId",
                            "cv.consent_form",
                            "Participant Contacts",
                        ]
                    ),
                },
            )
            for ripple_study in [
                v
                for k, v in ripple_variables.study_ids.items()
                if k in ["HBN - Main", "HBN - Waitlist"]
            ]
        ]
    )

    row_count = ripple_df.shape[0]
    logger.info("Ripple Returned Rows: %s", row_count)

    # Check if the returned DataFrame is empty to infer the API status.
    if ripple_df.empty:
        # If the DataFrame is empty, the API call may have failed or returned no data.
        logger.info("API request returned no data.")
        raise NoData

    # Filter the dataFrame on cv.consent_form and contact.2.infos.1.contactType
    filtered_ripple_df = ripple_df[(ripple_df["cv.consent_form"] == "Send to RedCap")]

    if filtered_ripple_df.empty:
        # If the DataFrame is empty, the API call may have failed or returned no data.
        logger.info('There are no participants marked "Send to RedCap".')
        raise NoData

    row_count = filtered_ripple_df.shape[0]
    logger.info(
        "API request successful and data received.\nRipple Filtered Rows: %s", row_count
    )

    return filtered_ripple_df


# ============================================================================
# Data Transformation
# ============================================================================


def set_redcap_columns(
    ripple_df: pd.DataFrame,
    columns_to_keep: list[str] = ["mrn", "email_consent"],
    columns_to_rename: dict = {"customId": "mrn"},
) -> pd.DataFrame:
    """
    Set appropriate columns. Prepends 'record_id' matching mrn.

    Define the columns you want to select:
    Ripple globalId, customId (MRN), contact.*.infos.*.information (contact email).
    Create a new dataframe with only the selected columns.
    """
    # Before renaming columns copy dataframe to create an independent DataFrame
    redcap_df = ripple_df.copy()

    contact_type_cols = [
        col for col in redcap_df.columns if col.endswith(".contactType")
    ]

    for col in contact_type_cols:
        col.replace(".contactType", ".information")

    # Create masks
    is_email = redcap_df[contact_type_cols] == "email"

    # Get first occurrence index per row
    first_match = is_email.idxmax(axis=1)

    # Get corresponding information
    redcap_df["email_consent"] = redcap_df.apply(
        lambda row: (
            row[first_match[row.name].replace(".contactType", ".information")]
            if is_email.loc[row.name].any()
            else pd.NA
        ),
        axis=1,
    )

    redcap_df.rename(columns=columns_to_rename, inplace=True)

    # Convert record_id and MRN to integers
    redcap_df["mrn"] = redcap_df["mrn"].astype(int)

    # Create the new column 'record_id' from the original 'customId'/mrn
    # 1. Create the new column 'record_id' and insert it at the first position (index 0)
    #    We are taking the values from the original 'customId' column.
    redcap_df.insert(0, "record_id", redcap_df["mrn"])

    return redcap_df[["record_id", *columns_to_keep]].drop_duplicates()


def get_redcap_subjects_to_update(
    df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Get subjects from REDCap that need to be updated vs. new subjects.

    Returns
    -------
    pd.DataFrame
        subjects to update
    pd.DataFrame
        new subjects

    """
    redcap_participant_consent_data = {
        "token": redcap_variables.Tokens().pid247,
        "content": "record",
        "action": "export",
        "format": "csv",
        "type": "flat",
        "csvDelimiter": "",
        "fields": "mrn,record_id",
        "rawOrLabel": "raw",
        "rawOrLabelHeaders": "raw",
        "exportCheckboxLabel": "false",
        "exportSurveyFields": "false",
        "exportDataAccessGroups": "false",
        "returnFormat": "csv",
    }

    df_redcap_consent_instruments = fetch_api_data(
        Endpoints.REDCap.base_url,
        redcap_variables.headers,
        redcap_participant_consent_data,
    )

    mask = df["mrn"].isin(df_redcap_consent_instruments["mrn"])

    to_update = pd.DataFrame(df[mask]).copy()
    to_update = to_update.drop(columns=["record_id"]).merge(
        df_redcap_consent_instruments[["mrn", "record_id"]], on="mrn", how="left"
    )
    to_update = pd.DataFrame(to_update[["record_id", "mrn", "email_consent"]]).copy()

    return to_update, pd.DataFrame(df[~mask]).copy()


def prepare_redcap_data(df: pd.DataFrame, cache: DataCache | None = None) -> None:
    """Prepare Ripple API returned data to be imported into REDCap."""
    copy_selected_redcap_df = set_redcap_columns(df)

    # Add last_modified column for cache keying
    if "lastModified" not in df.columns:
        last_modified = extract_last_modified(df)
        copy_selected_redcap_df = copy_selected_redcap_df.assign(
            lastModified=last_modified.values
        )

    # Filter out records already processed by cache
    if cache:
        # Create cache keys for each record
        cache_keys = []
        for _, row in copy_selected_redcap_df.iterrows():
            cache_key = create_ripple_record_cache_key(
                str(row["mrn"]),
                str(row.get("email_consent", "")),
                str(row.get("lastModified", "")),
            )
            cache_keys.append(cache_key)

        copy_selected_redcap_df["cache_key"] = cache_keys

        unprocessed_keys = cache.get_unprocessed_records(cache_keys)

        if len(unprocessed_keys) < len(copy_selected_redcap_df):
            logger.info(
                "Skipping %d already-processed records (cache hit)",
                len(copy_selected_redcap_df) - len(unprocessed_keys),
            )
            copy_selected_redcap_df = copy_selected_redcap_df[
                copy_selected_redcap_df["cache_key"].isin(unprocessed_keys)
            ]

    if copy_selected_redcap_df.empty:
        logger.info("No new records to prepare for REDCap")
        return

    # Split into update and new
    to_update, new_subjects = get_redcap_subjects_to_update(
        copy_selected_redcap_df.drop(columns=["cache_key"], errors="ignore")
    )

    # Save the new dataframes to CSV files
    if not to_update.empty:
        to_update.to_csv(redcap_variables.redcap_update_file, index=False)
    if not new_subjects.empty:
        new_subjects.to_csv(redcap_variables.redcap_import_file, index=False)

    # Mark as processed in cache (use the original df with cache_key)
    if cache and "cache_key" in copy_selected_redcap_df.columns:
        processed_keys = copy_selected_redcap_df["cache_key"].tolist()
        cache.bulk_mark_processed(
            processed_keys,
            metadata={
                "from_ripple": True,
                "num_updates": len(to_update),
                "num_new": len(new_subjects),
            },
        )


def prepare_ripple_to_ripple(df: pd.DataFrame) -> dict[str, str]:
    """Prepare Ripple API returned data to be re-imported (updated) in Ripple."""
    ripple_import_files: dict[str, str] = {}

    # Define the columns you want to select
    columns_to_keep_ripple = ["globalId", "cv.consent_form", "importType"]

    # Create a new dataframe with only the selected columns
    selected_ripple_df = df[columns_to_keep_ripple]

    for ripple_study in selected_ripple_df["importType"].unique():
        # Before updating column the cv.consent_form value
        # copy dataframe to create an independent DataFrame
        copy_selected_ripple_df = selected_ripple_df.copy()

        # Filter down to relevant rows
        copy_selected_ripple_df = copy_selected_ripple_df[
            copy_selected_ripple_df["importType"] == ripple_study
        ]

        copy_selected_ripple_df["cv.consent_form"] = "consent_form_created_in_redcap"

        ripple_import_dir = Path(ripple_variables.ripple_import_file).parent
        ripple_import_filepath = str(ripple_import_dir / f"{ripple_study}.xlsx")

        # Save the new dataframe to a Excel file
        copy_selected_ripple_df.to_excel(
            ripple_import_filepath, index=False, sheet_name="SentToRedCap"
        )

        ripple_import_files[ripple_study] = ripple_import_filepath

    return ripple_import_files


# ============================================================================
# REDCap Push
# ============================================================================


def _read_and_push_file(
    filepath: Path, project_token: str, update: bool
) -> tuple[int, str]:
    """
    Read a file and push to REDCap.

    Parameters
    ----------
    filepath : Path
        Path to CSV file to push
    project_token : str
        REDCap API token
    update : bool
        Whether this is an update operation

    Returns
    -------
    tuple[int, str]
        (status_code, response_text)

    """
    if not filepath.exists() or filepath.stat().st_size == 0:
        return (200, "0")  # Success with 0 records

    with open(filepath, "r") as file:
        csv_content = file.read()

    if not csv_content:
        return (200, "0")

    data = {
        "token": project_token,
        "content": "record",
        "action": "import",
        "format": "csv",
        "type": "flat",
        "overwriteBehavior": "normal",
        "forceAutoNumber": str(not update).lower(),
        "data": csv_content,
        "returnContent": "count",
        "returnFormat": "csv",
    }

    r = requests.post(Endpoints.REDCap.base_url, data=data)
    r.raise_for_status()

    return (r.status_code, r.text)


def push_to_redcap(project_token: str, update: Optional[bool] = None) -> None:
    """Push the HBN Potential Participants MRN and Contact email to REDCap."""
    if update is None:
        # Try both
        for _update in [True, False]:
            push_to_redcap(project_token, _update)
        return

    filepath = (
        redcap_variables.redcap_update_file
        if update
        else redcap_variables.redcap_import_file
    )

    status_code, response_text = _read_and_push_file(filepath, project_token, update)

    logger.info(
        "HTTP Status: %s\nRecords %s: %s",
        status_code,
        "Updated" if update else "Inserted",
        response_text,
    )


# ============================================================================
# Ripple Status Update
# ============================================================================


def _validate_ripple_import_file(ripple_import_file: str) -> bool:
    """
    Validate that Ripple import file has data.

    Parameters
    ----------
    ripple_import_file : str
        Path to Excel file

    Returns
    -------
    bool
        True if file has data, False otherwise

    """
    try:
        df = pd.read_excel(ripple_import_file)
        if df.empty:
            logger.info(
                "The Excel file %s is empty. No API request was sent.",
                ripple_import_file,
            )
            return False
        logger.info("File contains data. Proceeding with API request…")
        return True
    except FileNotFoundError as e:
        msg = f"Error: The file '{ripple_import_file}' was not found."
        raise FileNotFoundError(msg) from e
    except Exception:
        logger.exception("Error validating file %s", ripple_import_file)
        raise


def _send_ripple_import_request(
    study_import_url: str, file_content: bytes
) -> requests.Response:
    """
    Send import request to Ripple API.

    Parameters
    ----------
    study_import_url : str
        Ripple API import URL
    file_content : bytes
        File content to upload

    Returns
    -------
    requests.Response
        API response

    Raises
    ------
    requests.exceptions.HTTPError
        If request fails

    """
    response = requests.post(
        study_import_url,
        headers=ripple_variables.headers["import"],
        data=file_content,
    )

    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError:
        logger.debug("URL: %s", study_import_url)
        logger.debug("Content length: %d bytes", len(file_content))
        raise

    return response


def set_status_in_ripple(ripple_study: str, ripple_import_file: str) -> None:
    """
    Set the HBN Potential Participants consent form flag status.

    In **Consent Form Created in RedCap** in Ripple after RedCap Consent data push.
    """
    # Validate file
    if not _validate_ripple_import_file(ripple_import_file):
        return

    try:
        study_import_url = Endpoints.Ripple.import_data(ripple_study)

        with open(ripple_import_file, "rb") as ripple_file:
            file_content = ripple_file.read()
            response = _send_ripple_import_request(study_import_url, file_content)
            logger.info("Request was successful!\nResponse: %s", response.text)

    except requests.exceptions.RequestException as e:
        msg = f"An error occurred during the API request: {e}"
        raise requests.exceptions.RequestException(msg) from e
    except Exception:
        logger.exception("An unexpected error occurred")
        raise


# ============================================================================
# Cleanup
# ============================================================================


def cleanup(temp_files: list[str | Path]) -> None:
    """Delete temporary files."""
    for filepath in [
        redcap_variables.redcap_import_file,
        redcap_variables.redcap_update_file,
        *[Path(filepath) for filepath in temp_files],
    ]:
        try:
            filepath.unlink(missing_ok=True)
        except FileNotFoundError:
            logger.warning("%s already does not exist.", filepath)


# ============================================================================
# Main Pipeline
# ============================================================================


def main(project_status: ProjectStatus = "prod") -> None:
    """Transfer data from Ripple to REDCap."""
    # Initialize cache for minute-by-minute transfers (TTL: 2 minutes)
    cache = DataCache("ripple_to_redcap", ttl_minutes=2)

    project: dict[ProjectStatus, dict[str, str]] = {
        "dev": {"token": redcap_variables.Tokens().pid757},
        "prod": {"token": redcap_variables.Tokens().pid247},
    }

    ripple_import_files: dict[str, str] = {}

    try:
        filtered_ripple_df = request_potential_participants()
        prepare_redcap_data(filtered_ripple_df, cache)
        ripple_import_files = prepare_ripple_to_ripple(filtered_ripple_df)
        push_to_redcap(project[project_status]["token"])

        for ripple_study, ripple_import_file in ripple_import_files.items():
            set_status_in_ripple(ripple_study, ripple_import_file)

        # Log cache statistics
        log_cache_statistics(cache, logger)

    except NoData:
        logger.info("No data to transfer from Ripple")
    finally:
        cleanup(list(ripple_import_files.values()))


if __name__ == "__main__":
    main(project_status=Config.PROJECT_STATUS)
