"""Common functionality when fetching data from REDCap."""

from typing import Literal, Optional

import pandas as pd

from .._config_variables import redcap_variables
from ..exceptions import NoData
from ..utility_functions import fetch_api_data, initialize_logging

logger = initialize_logging(__name__)

Endpoints = redcap_variables.Endpoints()


def eav_to_wide(df: pd.DataFrame) -> pd.DataFrame:
    """Pivot EAV DataFrame to wide format."""
    return df.pivot_table(
        index="record", columns="field_name", values="value", aggfunc="first"
    ).reset_index()


def fetch_data(
    token: str,
    export_fields: str,
    filter_logic: Optional[str] = None,
    *,
    all_or_any: Literal["all", "any"] = "all",
    flat: bool = False,
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

    all_or_any
        match __all__ or __any__ `export_fields`

    flat
        return "flat" type instead of "eav" type?

    """
    redcap_participant_data = {
        "token": token,
        "content": "record",
        "action": "export",
        "format": "csv",
        "type": "flat" if flat else "eav",
        "csvDelimiter": "",
        "fields": export_fields,
        "rawOrLabel": "raw",
        "rawOrLabelHeaders": "raw",
        "exportCheckboxLabel": "false",
        "exportSurveyFields": "false",
        "exportDataAccessGroups": "false",
        "returnFormat": "csv",
    }
    orig_filter_logic = filter_logic
    if all_or_any == "any":
        filter_conditions = " OR ".join(
            [f"[{field}] != ''" for field in export_fields.split(",")]
        )
        filter_logic = (
            f"({filter_logic}) AND ({filter_conditions})"
            if filter_logic
            else filter_conditions
        )
    if filter_logic:
        redcap_participant_data["filterLogic"] = filter_logic
    df_redcap_participant_consent_data = fetch_api_data(
        Endpoints.base_url,
        redcap_variables.headers,
        redcap_participant_data,
        capture_invalid_fields=True,
    )
    if isinstance(df_redcap_participant_consent_data, list):
        export_list = [
            field
            for field in export_fields.split(",")
            if field not in df_redcap_participant_consent_data
        ]

        return fetch_data(
            token,
            export_fields=",".join(export_list),
            filter_logic=orig_filter_logic,
            all_or_any=all_or_any,
            flat=flat,
        )
    if df_redcap_participant_consent_data.empty:
        raise NoData

    if df_redcap_participant_consent_data.empty:
        logger.info(
            "There is not REDCap participant enrollment parental consent data "
            "to process."
        )
    return df_redcap_participant_consent_data


def get_responder_ids(df: pd.DataFrame) -> pd.DataFrame:
    """Get responder IDs from REDCap."""
    # TODO: Get responder IDs from PID 879.
    from .to_redcap import transform_redcap_data_for_responder_tracking  # noqa: PLC0415

    df, _u, _p = transform_redcap_data_for_responder_tracking()
    df["record"] = "R" + df["record"].astype(str).str.zfill(6)
    return df[["record", "resp_email"]]


def response_index_reverse_lookup(row: pd.Series) -> list[tuple[str, str, int | str]]:
    """Get response index reverse lookups from REDCap metadata."""
    field = row["field_name"]
    choices = row["select_choices_or_calculations"]
    lookups: list[tuple[str, str, int | str]] = []
    if pd.notna(choices):
        for choice in str(choices).split("|"):
            parts = choice.split(", ", 1)
            # index, key
            if len(parts) == 2:  # noqa: PLR2004
                value, label = parts
                try:
                    lookups.append(
                        (field, label.strip().lower(), str(int(value.strip())))
                    )
                except TypeError, ValueError:
                    lookups.append((field, label.strip().lower(), value.strip()))
    return lookups


__all__ = ["fetch_data", "response_index_reverse_lookup"]
