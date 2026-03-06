"""
Transfer data from REDCap to REDCap.

For each subject in PID 247, if `intake_ready` == 1:
- push subject to PID 744, &
- set `indake_ready` = 2 in PID 247.
"""

import pandas as pd

from .._config_variables import redcap_variables
from ..exceptions import NoData
from ..utility_functions import fetch_api_data, initialize_logging, redcap_api_push
from .config import Fields, Values

Endpoints = redcap_variables.Endpoints()

logger = initialize_logging(__name__)


def fetch_data(token: str, fields: str) -> pd.DataFrame:
    """Fetch data from REDCap API."""
    redcap_participant_consent_data = {
        "token": token,
        "content": "record",
        "action": "export",
        "format": "csv",
        "type": "eav",
        "csvDelimiter": "",
        "fields": fields,
        "filterLogic": "[intake_ready] = "
        f"{Values.PID247.intake_ready['Ready to Send to Intake Redcap']}",
        "rawOrLabel": "raw",
        "rawOrLabelHeaders": "raw",
        "exportCheckboxLabel": "false",
        "exportSurveyFields": "false",
        "exportDataAccessGroups": "false",
        "returnFormat": "csv",
    }

    df_redcap_participant_consent_data = fetch_api_data(
        Endpoints.base_url, redcap_variables.headers, redcap_participant_consent_data
    )
    if not df_redcap_participant_consent_data.shape[0]:
        raise NoData
    df_redcap_participant_consent_data["field_name"] = (
        df_redcap_participant_consent_data["field_name"].replace(
            Fields.rename_247_to_744
        )
    )

    if df_redcap_participant_consent_data.empty:
        logger.info(
            "There is not REDCap participant enrollment parental consent data "
            "to process."
        )
    return df_redcap_participant_consent_data


def update_source(df: pd.DataFrame) -> int:
    """
    Update `intake_ready` column in source project.

    Parameters
    ----------
    df
        destination DataFrame

    Returns
    -------
    int
        number of records updated

    """
    df_274 = pd.DataFrame(
        {
            "record": df["record"].unique(),
            "field_name": "intake_ready",
            "value": Values.PID247.intake_ready[
                "Participant information already sent to HBN - Intake Redcap project"
            ],
        }
    )
    return redcap_api_push(
        df=df_274,
        token=redcap_variables.Tokens.pid247,
        url=Endpoints.base_url,
        headers=redcap_variables.headers,
    )


def main() -> None:
    """Transfer data from REDCap to REDCap."""
    try:
        # get data from PID247
        data247 = fetch_data(redcap_variables.Tokens.pid247, str(Fields.export_247))
        if data247.empty:
            raise NoData
        # rename columns for PID744
        data247["field_name"] = data247["field_name"].replace(Fields.rename_247_to_744)
        # format DataFrame for PID744
        df_744 = data247.loc[
            data247["field_name"].str.startswith(tuple(Fields.import_744))
        ]
        assert isinstance(df_744, pd.DataFrame)
        df_744 = (
            df_744.sort_values("redcap_repeat_instance", ascending=False)
            .drop_duplicates(subset=["record", "field_name"], keep="first")
            .drop(columns=["redcap_repeat_instrument", "redcap_repeat_instance"])
            .reset_index(drop=True)
        )
        decrement_mask = df_744["field_name"] == "permission_collab"
        # Convert to numeric and decrement
        decremented = (
            pd.to_numeric(df_744.loc[decrement_mask, "value"], errors="coerce") - 1
        )
        assert isinstance(decremented, pd.Series)
        # Convert back to string
        df_744.loc[decrement_mask, "value"] = decremented.astype(str)
        rows_imported_744 = redcap_api_push(
            df=df_744,
            token=redcap_variables.Tokens.pid744,
            url=Endpoints.base_url,
            headers=redcap_variables.headers,
        )
        if not rows_imported_744:
            raise NoData
        rows_updated_274 = update_source(df_744)
        assert rows_imported_744 == rows_updated_274, (
            f"rows imported to PID 744 ({rows_imported_744}) "
            f"≠ rows updated in PID 274 ({rows_updated_274})."
        )
    except NoData:
        logger.info("No data to transfer from PID 274 to PID 744.")


if __name__ == "__main__":
    main()
