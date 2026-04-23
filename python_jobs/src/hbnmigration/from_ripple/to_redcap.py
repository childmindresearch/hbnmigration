"""Transfer data from Ripple to REDCap."""

from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import requests

from .._config_variables import redcap_variables, ripple_variables
from ..exceptions import NoData
from ..utility_functions import (
    fetch_api_data,
    initialize_logging,
    ProjectStatus,
    yesterday,
)

logger = initialize_logging(__name__)


class Endpoints:
    """API Endpoints."""

    REDCap = redcap_variables.Endpoints()
    """REDCap API endpoints."""
    Ripple = ripple_variables.Endpoints()
    """Riplle API endpoints."""


def request_potential_participants() -> pd.DataFrame:
    """Request Ripple potential participants data via Ripple API Export."""
    ripple_df = pd.concat(
        [
            Endpoints.Ripple.export_from_ripple(
                ripple_study,
                {
                    "surveyExportSince": yesterday,
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
    # filtered_ripple_df = ripple_df[ripple_df['cv.consent_form'] == 'Send to RedCap']
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
            else np.nan
        ),
        axis=1,
    )
    redcap_df.rename(columns=columns_to_rename, inplace=True)
    # Convert record_id and MRN to integers
    redcap_df["mrn"] = redcap_df["mrn"].astype(int)
    # Create the new column 'record_id' from the original 'customId'/mrn
    # selected_ripple_df['record_id'] = selected_ripple_df['customId']
    # 1. Create the new column 'record_id' and insert it at the first position (index 0)
    #    We are taking the values from the original 'customId' column.
    redcap_df.insert(0, "record_id", redcap_df["mrn"])

    return redcap_df[["record_id", *columns_to_keep]].drop_duplicates()


def prepare_redcap_data(df: pd.DataFrame) -> None:
    """Prepare Ripple API returned data to be imported into REDCap."""
    copy_selected_redcap_df = set_redcap_columns(df)

    # Split into update and new
    to_update, new_subjects = get_redcap_subjects_to_update(copy_selected_redcap_df)

    # Save the new dataframes to CSV files
    if not to_update.empty:
        to_update.to_csv(redcap_variables.redcap_update_file, index=False)
    if not new_subjects.empty:
        new_subjects.to_csv(redcap_variables.redcap_import_file, index=False)


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
        "token": redcap_variables.Tokens.pid247,
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


def push_to_redcap(project_token: str, update: Optional[bool] = None) -> None:
    """Push the HBN Potential Participants MRN and Contact email to RedCap."""
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
    if not filepath.exists() or filepath.stat().st_size == 0:
        return
    with open(filepath, "r") as file:
        csv_content = file.read()
    if csv_content:
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
        # Raise exception if push fails
        r.raise_for_status()
        logger.info("HTTP Status: %s\nRecords Inserted: %s", r.status_code, r.text)


def set_status_in_ripple(ripple_study: str, ripple_import_file: str) -> None:
    """
    Set the HBN Potential Participants consent form flag status.

    In **Consent Form Created in RedCap** in Ripple after RedCap Consent data push.
    """
    try:
        # Read the Excel file into a pandas DataFrame first to check its contents
        df = pd.read_excel(ripple_import_file)

        # Check if the DataFrame is not empty
        if df.empty:
            logger.info(
                "The Excel file %s is empty. No API request was sent.",
                ripple_import_file,
            )
            return
        logger.info("File contains data. Proceeding with API request…")
        study_import_url = Endpoints.Ripple.import_data(ripple_study)
        with open(ripple_import_file, "rb") as ripple_file:
            file_content = ripple_file.read()
            response = requests.post(
                study_import_url,
                headers=ripple_variables.headers["import"],
                data=file_content,
            )
            try:
                # Raise an exception for bad status codes (4xx or 5xx)
                response.raise_for_status()
            except requests.exceptions.HTTPError:
                logger.debug(study_import_url)
                logger.debug(file_content)
                raise
            logger.info("Request was successful!\nResponse: %s", response.text)
    except FileNotFoundError as e:
        msg = f"Error: The file '{ripple_import_file}' was not found."
        raise FileNotFoundError(msg) from e
    except requests.exceptions.RequestException as e:
        # This will catch network-related errors like connection errors, timeouts, etc.
        msg = f"An error occurred during the API request: {e}"
        raise requests.exceptions.RequestException(msg) from e
    except Exception as e:
        # A general exception for other potential errors
        # (e.g., file is not a valid Excel file)
        logger.exception("An unexpected error occurred: %s", e)  # noqa: TRY401
        raise


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


def main(project_status: ProjectStatus = "prod") -> None:
    """Transfer data from Ripple to REDCap."""
    project: dict[ProjectStatus, dict[str, str]] = {
        "dev": {"token": redcap_variables.Tokens.pid757},
        "prod": {"token": redcap_variables.Tokens.pid247},
    }
    ripple_import_files: dict[str, str] = {}
    try:
        filtered_ripple_df = request_potential_participants()
        prepare_redcap_data(filtered_ripple_df)
        ripple_import_files = prepare_ripple_to_ripple(filtered_ripple_df)
        push_to_redcap(project[project_status]["token"])
        for ripple_study, ripple_import_file in ripple_import_files.items():
            set_status_in_ripple(ripple_study, ripple_import_file)
    except NoData:
        pass
    finally:
        cleanup(list(ripple_import_files.values()))


if __name__ == "__main__":
    main(project_status="prod")
