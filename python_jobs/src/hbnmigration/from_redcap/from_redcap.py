"""Common functionality when fetching data from REDCap."""

from typing import Hashable, Literal, Optional

import pandas as pd

from .._config_variables import redcap_variables
from ..exceptions import NoData
from ..utility_functions import ColumnRenameMapping, fetch_api_data, initialize_logging
from .config import Fields, Values

logger = initialize_logging(__name__)

_EmailToRecord = dict[Hashable, str | int]
Endpoints = redcap_variables.Endpoints()
Tokens = redcap_variables.Tokens()

RESPONDER_PARTICIPANT_PAIRINGS = [
    ("responder", "child", "child"),
    ("responder2", "child", "child"),
    ("responder_adult1", "adult_participant", "adult"),
    ("responder_adult2", "adult_participant", "adult"),
]


def eav_to_wide(df: pd.DataFrame) -> pd.DataFrame:
    """Pivot EAV DataFrame to wide format."""
    return df.pivot_table(
        index="record", columns="field_name", values="value", aggfunc="first"
    ).reset_index()


def build_responders_df() -> pd.DataFrame:
    """
    Build a combined responders DataFrame from REDCap consent data.

    Returns a DataFrame with deduplicated responder records including
    'record' and 'resp_email' columns (among others).
    """
    df = get_data_for_responder_tracking()
    mappings = Fields.rename.redcap_consent_to_redcap_responder_tracking()
    wide_df = eav_to_wide(df)
    all_responders, _ = _pivot_and_map_pairings(
        wide_df, mappings, RESPONDER_PARTICIPANT_PAIRINGS
    )
    return combine_all_responders(all_responders)


def calculate_repeat_instance(
    existing_participants_df: pd.DataFrame, participants_df: pd.DataFrame
) -> pd.DataFrame:
    """Calculate repeat instances based on existing data."""
    if existing_participants_df is not None and not existing_participants_df.empty:
        # Get max repeat instance for each existing record
        max_instances = (
            existing_participants_df.groupby("record")["redcap_repeat_instance"]
            .max()
            .to_dict()
        )

        # For each responder, get their current max instance
        participants_df["current_max_instance"] = (
            participants_df["record"].map(max_instances).fillna(0).astype(int)
        )

        # Assign new repeat instances starting from current max + 1
        participants_df["redcap_repeat_instance"] = (
            participants_df.groupby("record").cumcount()
            + participants_df["current_max_instance"]
            + 1
        )

        participants_df = participants_df.drop(columns=["current_max_instance"])
    else:
        # No existing data, start from 1
        participants_df["redcap_repeat_instance"] = (
            participants_df.groupby("resp_email").cumcount() + 1
        )
    return participants_df


def combine_all_participants(
    all_participants: list[pd.DataFrame],
    existing_responders_df: pd.DataFrame,
    email_to_record: _EmailToRecord,
) -> pd.DataFrame:
    """Combine all child or adult participants into a single DataFrame."""
    if all_participants:
        participants_df = pd.concat(all_participants, ignore_index=True)
        # Keep only necessary columns
        base_cols = [
            "resp_email",
            "child_dob",
            "child_fname",
            "mrn",
            "participant_type",
            "source_record",
        ]
        participants_df = participants_df[
            [col for col in base_cols if col in participants_df.columns]
        ]
        # Remove rows where email is missing
        participants_df = participants_df.dropna(subset=["resp_email"])
        participants_df = participants_df[
            participants_df["resp_email"].str.strip() != ""
        ]

        # Normalize email for matching
        participants_df["resp_email_lower"] = (
            participants_df["resp_email"].str.lower().str.strip()
        )

        # Map participants to responder records (both existing and new)
        if existing_responders_df is not None and not existing_responders_df.empty:
            participants_df["record"] = participants_df["resp_email_lower"].map(
                email_to_record
            )
        else:
            participants_df["record"] = None
        existing_participants_df = get_existing_participants_from_api()
        participants_df = calculate_repeat_instance(
            existing_participants_df, participants_df
        ).drop(columns=["resp_email_lower"])

        # Reorder columns
        col_order = [
            "record",
            "resp_email",
            "redcap_repeat_instance",
            "child_dob",
            "child_fname",
            "mrn",
            "participant_type",
            "source_record",
        ]
        participants_df = participants_df[
            [col for col in col_order if col in participants_df.columns]
        ]

    else:
        participants_df = pd.DataFrame(
            columns=[
                "record",
                "resp_email",
                "redcap_repeat_instance",
                "child_dob",
                "child_fname",
                "mrn",
                "participant_type",
            ]
        )
    return participants_df


def combine_all_responders(all_responders: list[pd.DataFrame]) -> pd.DataFrame:
    """Combine all responders (responder (1 | 2) × (child | adult) participant)."""
    if all_responders:
        responders_df = pd.concat(all_responders, ignore_index=True)
        # Remove rows where email is missing or empty
        responders_df = responders_df.dropna(subset=["resp_email"])
        responders_df = responders_df[responders_df["resp_email"].str.strip() != ""]

        # Deduplicate by email (keep first occurrence with most complete data)
        responders_df = responders_df.sort_values(
            by=["resp_email", "resp_fname", "resp_lname", "resp_phone"],
            na_position="last",
        )
        responders_df = responders_df.drop_duplicates(
            subset=["resp_email"], keep="first"
        )
    else:
        responders_df = pd.DataFrame(
            columns=["resp_email", "resp_fname", "resp_lname", "resp_phone"]
        )
    return responders_df


def get_data_for_responder_tracking() -> pd.DataFrame:
    """Get data from intake project to transfer to responder tracking project."""
    data247 = fetch_data(
        Tokens.pid247,
        str(Fields.export_247.for_redcap_responder_tracking),
        Values.PID247.intake_ready.filter_logic("Ready to Send to Intake Redcap"),
    )
    if data247.empty:
        raise NoData
    return data247


def get_existing_responders_from_api() -> pd.DataFrame:
    """Fetch existing responders from REDCap API."""
    # TODO: Implement API call to fetch existing responders
    return pd.DataFrame({}, columns=["record", "resp_email"])


def get_existing_participants_from_api() -> pd.DataFrame:
    """
    Fetch existing participants from REDCap API.

    Note:
    ----
    `"record"` here refers to the responder's record ID.

    """
    # TODO: Implement API call to fetch existing participants (repeating instrument)
    return pd.DataFrame({}, columns=["record", "mrn"])


def split_responders(
    responders_df: pd.DataFrame, existing_responders_df: Optional[pd.DataFrame] = None
) -> tuple[pd.DataFrame, pd.DataFrame, _EmailToRecord]:
    """Split `all` responders into `create` and `update` based on `existing`."""
    email_to_record: _EmailToRecord = {}
    if existing_responders_df is not None and not existing_responders_df.empty:
        # Normalize email addresses for comparison
        existing_responders_df["resp_email_lower"] = (
            existing_responders_df["resp_email"].str.lower().str.strip()
        )
        responders_df["resp_email_lower"] = (
            responders_df["resp_email"].str.lower().str.strip()
        )

        # Create mapping of email to existing record ID
        email_to_record = existing_responders_df.set_index("resp_email_lower")[
            "record"
        ].to_dict()

        # Check which responders already exist
        responders_df["existing_record"] = responders_df["resp_email_lower"].map(
            email_to_record
        )

        # Split into update and create
        update_responders_df = responders_df[
            responders_df["existing_record"].notna()
        ].copy()
        update_responders_df["record"] = update_responders_df["existing_record"].astype(
            int
        )
        update_responders_df = update_responders_df.drop(
            columns=["existing_record", "resp_email_lower"]
        )

        create_responders_df = responders_df[
            responders_df["existing_record"].isna()
        ].copy()
        create_responders_df = create_responders_df.drop(
            columns=["existing_record", "resp_email_lower"]
        )

    else:
        create_responders_df = responders_df.copy()
        update_responders_df = pd.DataFrame(
            columns=["record", "resp_email", "resp_fname", "resp_lname", "resp_phone"]
        )
    return create_responders_df, update_responders_df, email_to_record


def transform_redcap_data_for_responder_tracking() -> tuple[
    pd.DataFrame, pd.DataFrame, pd.DataFrame
]:
    """
    Transform EAV DataFrame from REDCap consent to responder tracking format.

    Returns
    -------
    create_responders_df
        DataFrame with new responders to create
    update_responders_df
        DataFrame with existing responders to update
    participants_df
        DataFrame with participants linked to responders

    """
    df = get_data_for_responder_tracking()
    mappings = Fields.rename.redcap_consent_to_redcap_responder_tracking()
    wide_df = eav_to_wide(df)

    all_responders, all_participants = _pivot_and_map_pairings(
        wide_df, mappings, RESPONDER_PARTICIPANT_PAIRINGS
    )

    responders_df = combine_all_responders(all_responders)
    existing_responders_df = get_existing_responders_from_api()
    create_responders_df, update_responders_df, email_to_record = split_responders(
        responders_df, existing_responders_df
    )
    participants_df = combine_all_participants(
        all_participants, existing_responders_df, email_to_record
    )
    return create_responders_df, update_responders_df, participants_df


def fetch_data(
    token: str,
    export_fields: Optional[str] = None,
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
        "rawOrLabel": "raw",
        "rawOrLabelHeaders": "raw",
        "exportCheckboxLabel": "false",
        "exportSurveyFields": "false",
        "exportDataAccessGroups": "false",
        "returnFormat": "csv",
    }
    if export_fields:
        redcap_participant_data["fields"] = export_fields
    orig_filter_logic = filter_logic
    if all_or_any == "any" and export_fields:
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
    if isinstance(df_redcap_participant_consent_data, list) and export_fields:
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


def _pivot_and_map_pairings(
    wide_df: pd.DataFrame,
    mappings: ColumnRenameMapping,
    pairings: list[tuple[str, str, str]],
) -> tuple[list[pd.DataFrame], list[pd.DataFrame]]:
    """
    Extract and rename the relevant columns from a wide-format DataFrame.

    Returns
    -------
    all_responders
        List of DataFrames with renamed responder fields.
    all_participants
        List of DataFrames with renamed responder + participant fields.

    """
    all_responders = []
    all_participants = []

    for responder_map_name, participant_map_name, participant_type in pairings:
        responder_map = getattr(mappings, responder_map_name)
        participant_map = getattr(mappings, participant_map_name)

        responder_cols = [col for col in responder_map if col in wide_df.columns]
        participant_cols = [col for col in participant_map if col in wide_df.columns]

        if not responder_cols or not participant_cols:
            continue

        relevant_data = wide_df.loc[
            :, ["record", *responder_cols, *participant_cols]
        ].copy()
        relevant_data = relevant_data.dropna(subset=responder_cols, how="all")

        if relevant_data.empty:
            continue

        # Responders
        responder_data = relevant_data.loc[:, ["record", *responder_cols]].copy()
        responder_data = responder_data.rename(
            columns={
                old: new
                for old, new in responder_map.items()
                if old in responder_data.columns
            }
        )
        all_responders.append(responder_data)

        # Participants
        participant_data = relevant_data.copy()
        participant_data = participant_data.rename(
            columns={
                old: new
                for old, new in responder_map.items()
                if old in participant_data.columns
            }
        )
        participant_data = participant_data.rename(
            columns={
                old: new
                for old, new in participant_map.items()
                if old in participant_data.columns
            }
        )
        participant_data["participant_type"] = participant_type
        participant_data["source_record"] = participant_data["record"]
        all_participants.append(participant_data)

    return all_responders, all_participants


def get_responder_ids(df: pd.DataFrame) -> pd.DataFrame:
    """Get responder IDs from REDCap."""
    responders = build_responders_df()
    responders["record"] = "R" + responders["record"].astype(str).str.zfill(6)
    return responders[["record", "resp_email"]]


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
