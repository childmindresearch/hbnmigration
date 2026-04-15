"""
Transfer data from REDCap to REDCap.

For each subject in PID 247, if `intake_ready` == 1:
- push subject to PID 744 / 625, &
- set `indake_ready` = 2 in PID 247.
"""

from typing import Hashable, Optional

import pandas as pd

from .._config_variables import redcap_variables
from ..config import Config
from ..exceptions import NoData
from ..utility_functions import DataCache, initialize_logging, redcap_api_push
from .config import Fields, Values
from .from_redcap import eav_to_wide, fetch_data

Endpoints = redcap_variables.Endpoints()
Tokens = redcap_variables.Tokens()
logger = initialize_logging(__name__)
_EmailToRecord = dict[Hashable, str | int]


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


def get_data_for_responder_tracking() -> pd.DataFrame:
    """Get data from intake project to transfer to responder tracking project."""
    data247 = fetch_data(
        Tokens.pid247,
        str(Fields.export_247.for_redcap_responder_tracking),
        # Values.PID247.intake_ready.filter_logic("Ready to Send to Intake Redcap"),
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


def push_new_responders() -> None:
    """Push new responders to PID 879."""
    # TODO: Implement


def update_existing_responders() -> None:
    """Update existing responders to PID 879."""


def create_participant_key(source_record: str, mrn: str) -> Optional[tuple[str, str]]:
    """
    Create a composite key to uniquely identify a participant.

    Returns
    -------
    Tuple of (source_record, mrn) or None if both are missing

    """
    if pd.notna(source_record) and pd.notna(mrn):
        record_val = str(source_record).strip()
        mrn_val = str(mrn).strip()
        return record_val, mrn_val
    return None


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
        DataFrame with participants linked to responders (with correct repeat instances)

    """
    df = get_data_for_responder_tracking()
    mappings = Fields.rename.redcap_consent_to_redcap_responder_tracking()

    # Define all responder-participant pairings
    pairings = [
        ("responder", "child", "child"),
        ("responder2", "child", "child"),
        ("responder_adult1", "adult_participant", "adult"),
        ("responder_adult2", "adult_participant", "adult"),
    ]

    all_responders = []
    all_participants = []

    # Pivot entire dataframe to wide format once
    wide_df = eav_to_wide(df)

    for responder_map_name, participant_map_name, participant_type in pairings:
        responder_map = getattr(mappings, responder_map_name)
        participant_map = getattr(mappings, participant_map_name)

        # Get all field names for this mapping
        responder_fields = list(responder_map.keys())
        participant_fields = list(participant_map.keys())

        # Check which fields exist in the data
        responder_cols: list[str] = [
            col for col in responder_fields if col in wide_df.columns
        ]
        participant_cols: list[str] = [
            col for col in participant_fields if col in wide_df.columns
        ]

        if not responder_cols or not participant_cols:
            continue

        # Extract relevant data
        relevant_data = wide_df.loc[
            :, ["record", *responder_cols, *participant_cols]
        ].copy()

        # Drop rows where all responder fields are null
        relevant_data = relevant_data.dropna(subset=responder_cols, how="all")

        if relevant_data.empty:
            continue

        # Process responder data
        responder_data = relevant_data.loc[:, ["record", *responder_cols]].copy()
        for old_name, new_name in responder_map.items():
            if old_name in responder_data.columns:
                responder_data = responder_data.rename(columns={old_name: new_name})

        all_responders.append(responder_data)

        # Process participant data
        participant_data = relevant_data.copy()

        # Rename responder fields
        for old_name, new_name in responder_map.items():
            if old_name in participant_data.columns:
                participant_data = participant_data.rename(columns={old_name: new_name})

        # Rename participant fields
        for old_name, new_name in participant_map.items():
            if old_name in participant_data.columns:
                participant_data = participant_data.rename(columns={old_name: new_name})

        participant_data["participant_type"] = participant_type
        participant_data["source_record"] = participant_data["record"]

        all_participants.append(participant_data)

    responders_df = combine_all_responders(all_responders)
    existing_responders_df = get_existing_responders_from_api()
    create_responders_df, update_responders_df, email_to_record = split_responders(
        responders_df, existing_responders_df
    )
    participants_df = combine_all_participants(
        all_participants, existing_responders_df, email_to_record
    )
    return create_responders_df, update_responders_df, participants_df


def update_source(df: pd.DataFrame, record_ids: dict[int | str, int | str]) -> int:
    """
    Update `intake_ready` column in source project.

    Parameters
    ----------
    df
        destination DataFrame

    record_ids
        mapping of record_ids between two REDCap projects

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
    df_274["record"] = df_274["record"].replace({v: k for k, v in record_ids.items()})
    return redcap_api_push(
        df=df_274,
        token=Tokens.pid247,
        url=Endpoints.base_url,
        headers=redcap_variables.headers,
    )


def update_complete_parent_second_guardian_consent(df: pd.DataFrame) -> pd.DataFrame:
    """
    Update `"parent_second_guardian_consent_complete"` based on `"guardian2_consent"`.

    Only records whose `"guardian2_consent"` value is in `mapping` are affected.
    All other records are left unchanged.
    """
    mapping = {
        Values.PID247.guardian2_consent[
            _consent
        ]: Values.PID625.complete_parent_second_guardian_consent[_operations]
        for _consent, _operations in [
            ("No", "Not Required"),
            (
                "Not Applicable (Adult Participant)",
                "Not Applicable (Adult Participant)",
            ),
        ]
    }
    # compute desired target value per record
    record_to_value = (
        df.query("field_name == 'guardian2_consent'")
        .set_index("record")["value"]
        .map(mapping)
        .dropna()
    )
    if record_to_value.empty:
        return df
    records_to_update = record_to_value.index

    # update existing rows
    mask = (df["field_name"] == "complete_parent_second_guardian_consent") & (
        df["record"].isin(records_to_update)
    )
    df.loc[mask, "value"] = df.loc[mask, "record"].map(record_to_value)

    # append missing rows
    missing_records = records_to_update.difference(
        df.loc[
            df["field_name"] == "complete_parent_second_guardian_consent", "record"
        ].tolist()
    )
    if len(missing_records):
        df = pd.concat(
            [
                df,
                pd.DataFrame(
                    {
                        "record": missing_records,
                        "field_name": "complete_parent_second_guardian_consent",
                        "value": record_to_value.loc[missing_records].values,
                    }
                ),
            ],
            ignore_index=True,
        )
    df = df.sort_values(["record", "field_name"], kind="stable").reset_index(drop=True)
    return df.sort_values(["record", "field_name"], kind="stable").reset_index(
        drop=True
    )


def main() -> None:
    """Transfer data from REDCap to REDCap."""
    # Initialize cache for minute-by-minute transfers (TTL: 2 minutes)
    cache = DataCache("redcap_to_redcap", ttl_minutes=2)

    try:
        # get data from PID247
        data247 = fetch_data(
            Tokens.pid247,
            str(Fields.export_247.for_redcap_operations),
            Values.PID247.intake_ready.filter_logic("Ready to Send to Intake Redcap"),
        )
        data247["field_name"] = data247["field_name"].replace(
            Fields.rename.redcap_consent_to_redcap_operations
        )
        if data247.empty:
            raise NoData

        # Filter out records already processed by cache
        unique_records = data247["record"].unique()
        unprocessed_records = cache.get_unprocessed_records(unique_records.tolist())

        if len(unprocessed_records) < len(unique_records):
            logger.info(
                "Skipping %d already-processed records (cache hit)",
                len(unique_records) - len(unprocessed_records),
            )
            data247 = data247[data247["record"].isin(unprocessed_records)]

        if data247.empty:
            logger.info("All records already processed in cache.")
            return

        # rename columns for consent project
        data247 = update_complete_parent_second_guardian_consent(data247)
        data247["field_name"] = data247["field_name"].replace(
            Fields.rename.redcap_consent_to_redcap_operations
        )
        # format DataFrame for operations project
        df_operations = data247.loc[
            data247["field_name"].str.startswith(tuple(Fields.import_625))
        ]
        record_ids: dict[int | str, int | str] = {
            row["record"]: row["value"]
            for _, row in df_operations[df_operations["field_name"] == "mrn"].iterrows()
        }
        df_operations["record"] = df_operations["record"].replace(record_ids)
        df_operations.loc[df_operations["field_name"] == "record_id", "value"] = (
            df_operations.loc[df_operations["field_name"] == "record_id", "record"]
        )
        assert isinstance(df_operations, pd.DataFrame)
        df_operations = (
            df_operations.sort_values("redcap_repeat_instance", ascending=False)
            .drop_duplicates(subset=["record", "field_name"], keep="first")
            .drop(columns=["redcap_repeat_instrument", "redcap_repeat_instance"])
            .reset_index(drop=True)
        )
        decrement_mask = df_operations["field_name"] == "permission_collab"
        # Convert to numeric and decrement
        decremented = (
            pd.to_numeric(df_operations.loc[decrement_mask, "value"], errors="coerce")
            - 1
        )
        assert isinstance(decremented, pd.Series)
        # Convert back to string
        df_operations.loc[decrement_mask, "value"] = decremented.astype(str)
        rows_imported_operations = redcap_api_push(
            df=df_operations,
            token=getattr(
                redcap_variables.Tokens,
                "pid625" if Config.PROJECT_STATUS == "prod" else "pid744",
            ),
            url=Endpoints.base_url,
            headers=redcap_variables.headers,
        )
        if not rows_imported_operations:
            raise NoData

        # Mark source records as processed in cache
        source_records = data247["record"].unique().tolist()
        cache.bulk_mark_processed(
            source_records,
            metadata={"rows_imported": rows_imported_operations},
        )

        rows_updated_274 = update_source(df_operations, record_ids)
        assert rows_imported_operations == rows_updated_274, (
            f"rows imported to REDCap operations ({rows_imported_operations}) "
            f"≠ rows updated in REDCap consent ({rows_updated_274})."
        )

        # Log cache statistics
        cache_stats = cache.get_stats()
        logger.info(
            "Cache statistics: %d entries, file size: %d bytes, last activity: %s",
            cache_stats["total_entries"],
            cache_stats["file_size_bytes"],
            cache_stats.get("last_activity", "never"),
        )
    except NoData:
        logger.info(
            "No data to transfer from REDCap consent project to "
            "REDCap operations project."
        )


if __name__ == "__main__":
    main()
