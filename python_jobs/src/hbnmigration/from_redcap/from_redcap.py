"""Common functionality when fetching data from REDCap."""

from typing import Optional

import pandas as pd

from .._config_variables import redcap_variables
from ..exceptions import NoData
from ..utility_functions import fetch_api_data, initialize_logging

logger = initialize_logging(__name__)

Endpoints = redcap_variables.Endpoints()


def fetch_data(
    token: str, export_fields: str, filter_logic: Optional[str] = None
) -> pd.DataFrame:
    """
    Fetch data from REDCap API.

    Parameters
    ----------
    token
        REDCap project API token

    export_fields
        comma-delimited list of REDCap fields to export

    filter_logic
        REDCap-API-syntax `filterLogic`

    """
    redcap_participant_data = {
        "token": token,
        "content": "record",
        "action": "export",
        "format": "csv",
        "type": "eav",
        "csvDelimiter": "",
        "fields": export_fields,
        "rawOrLabel": "raw",
        "rawOrLabelHeaders": "raw",
        "exportCheckboxLabel": "false",
        "exportSurveyFields": "false",
        "exportDataAccessGroups": "false",
        "returnFormat": "csv",
    }
    if filter_logic:
        redcap_participant_data["filterLogic"] = filter_logic

    df_redcap_participant_consent_data = fetch_api_data(
        Endpoints.base_url, redcap_variables.headers, redcap_participant_data
    )
    if df_redcap_participant_consent_data.empty:
        raise NoData

    if df_redcap_participant_consent_data.empty:
        logger.info(
            "There is not REDCap participant enrollment parental consent data "
            "to process."
        )
    return df_redcap_participant_consent_data


def response_index_reverse_lookup(row: pd.Series) -> Optional[tuple[str, str, int]]:
    """Get response index reverse lookups from REDCap metadata."""
    field = row["field_name"]
    choices = row["select_choices_or_calculations"]

    if pd.notna(choices):
        for choice in str(choices).split("|"):
            parts = choice.strip().split(",", 1)
            # index, key
            if len(parts) == 2:  # noqa: PLR2004
                value, label = parts
                try:
                    return field, label.strip().lower(), int(value.strip())
                except (TypeError, ValueError):
                    pass
    return None


__all__ = ["fetch_data"]
