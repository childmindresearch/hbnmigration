"""
Transfer data from REDCap to Curious.

For each subject in PID 625, if `enrollment_complete` == 1,
prepares and copies the reviewed and approved participants by the RAs to Curious.
"""

from typing import Literal

import numpy as np
import pandas as pd
import requests

from .._config_variables import curious_variables, redcap_variables
from ..exceptions import NoData
from ..from_curious.config import AccountType
from ..utility_functions import (
    DataCache,
    initialize_logging,
    new_curious_account,
    redcap_api_push,
)
from .config import Fields, Values
from .from_redcap import fetch_data, get_responder_ids

logger = initialize_logging(__name__)

INDIVIDUALS: list[Literal["child", "parent"]] = ["parent", "child"]

_REDCAP_TOKENS = redcap_variables.Tokens()
_REDCAP_PID = 247


def _in_set(x: set | int | str, required_value: int | str = 1) -> bool:
    """Check if required value in "parental_involvement" column."""
    if isinstance(x, (int, str)):
        x = {x}
    if not isinstance(x, (list, set)):
        return False
    return str(required_value) in [str(_) for _ in x]


def _check_for_data_to_process(df: pd.DataFrame, account_type: AccountType) -> None:
    """Check for data to process and log result."""
    if df.loc[df["accountType"] == account_type].empty:
        logger.info("There is not %s consent data to process.", account_type)
    else:
        logger.info(
            "%s data was prepared to be sent to the Curious API.",
            account_type.capitalize(),
        )


def _format_redcap_data_for_curious(
    redcap_data: pd.DataFrame, individual: Literal["child", "parent"]
) -> pd.DataFrame:
    """For a class of individual, format REDCap data for Curious."""
    record_set: set[int | str] = set()
    df_temp = pd.DataFrame(redcap_data[["record", "field_name", "value"]]).copy()
    df_temp["field_name"] = df_temp["field_name"].replace(
        getattr(Fields.rename.redcap247_to_curious, individual)
    )

    # Filter to relevant fields
    individual_fields: dict[str, int | str | None] = getattr(
        Fields.import_curious, individual
    )
    relevant_fields = list(individual_fields.keys())
    df_temp = df_temp[df_temp["field_name"].isin(relevant_fields)]
    df_temp = (
        df_temp.groupby(["record", "field_name"])["value"]
        .apply(lambda x: set(x) if len(x) > 1 else x.iloc[0])
        .reset_index()
    )

    # Pivot
    df_pivoted = df_temp.pivot(index="record", columns="field_name", values="value")
    record_set = {*record_set, *df_pivoted.index.tolist()}
    # Add missing columns with defaults
    for field, default_value in individual_fields.items():
        if field not in df_pivoted.columns:
            df_pivoted[field] = default_value

    # For parent, modify secretUserId column
    if individual == "parent" and "secretUserId" in df_pivoted.columns:
        _r_lookups = redcap_data.copy()
        for responder in [
            "responder",
            "responder2",
            "responder_adult1",
            "responder_adult2",
        ]:
            _r_lookups["field_name"] = _r_lookups["field_name"].replace(
                getattr(
                    Fields.rename.redcap_consent_to_redcap_responder_tracking, responder
                )
            )
        _responder_ids = get_responder_ids(_r_lookups)
        df_pivoted["secretUserId"] = (
            df_pivoted["email"]
            .str.lower()
            .replace(
                dict(
                    zip(
                        _responder_ids["resp_email"].str.lower(),
                        _responder_ids["record"],
                    )
                )
            )
        )
    df = df_pivoted.reset_index(drop=True)
    return df.where(pd.notna(df), np.nan)


def format_redcap_data_for_curious(
    redcap_data: pd.DataFrame,
) -> dict[Literal["child", "parent"], pd.DataFrame]:
    """Format REDCap export data for Curious import."""
    curious_participant_data: dict[Literal["child", "parent"], pd.DataFrame] = {
        individual: _format_redcap_data_for_curious(redcap_data, individual)
        for individual in INDIVIDUALS
    }
    if "parent_involvement" in curious_participant_data["child"].columns:
        curious_participant_data["child"] = pd.DataFrame(
            curious_participant_data["child"][
                curious_participant_data["child"]["parent_involvement"].apply(_in_set)
                | curious_participant_data["child"]["parent_involvement"].isna()
            ]
        ).dropna(axis=1, how="all")

    # Now drop `parent_involvement` column before we push to Curious.
    curious_participant_data["child"] = curious_participant_data["child"].drop(
        columns=["parent_involvement", "adult_enrollment_form_complete"],
        errors="ignore",
    )

    # Pad `secretUserId` with leading zeros to make it 5 characters long
    curious_participant_data["child"]["secretUserId"] = (
        curious_participant_data["child"]["secretUserId"].astype(str).str.zfill(5)
    )
    return curious_participant_data


def send_to_curious(
    df: pd.DataFrame,
    tokens: curious_variables.Tokens,
    applet_id: str,
    cache: DataCache | None = None,
) -> list[str]:
    """Send new participants to Curious."""
    failures: list[str] = []
    headers = curious_variables.headers(tokens.access)

    # Loop through each REDCap transformed record and sent it to MindLogger
    for record in [
        {k: v for k, v in record.items() if v is not None}
        for record in df.to_dict(orient="records")
    ]:
        secret_user_id = record.get("secretUserId", "")
        mrn = stringify_secret_user_id(secret_user_id) if secret_user_id else ""

        # Check cache before sending
        if cache and cache.is_processed(mrn):
            logger.info(
                "Skipping MRN %s (already sent to Curious)",
                mrn,
            )
            continue

        try:
            logger.info(
                "%s",
                new_curious_account(
                    tokens.endpoints.base_url, applet_id, record, headers
                ),
            )
            # Mark as processed in cache
            if cache:
                cache.mark_processed(mrn, metadata={"sent_to_curious": True})
        except requests.exceptions.RequestException:
            logger.exception("Error")
            failures.append(mrn)
    return failures


def stringify_secret_user_id(secret_user_id: int | str) -> str:
    """Return string with leading zeroes dropped."""
    try:
        return str(int(secret_user_id))
    except TypeError, ValueError:
        return str(secret_user_id)


def update_redcap(
    redcap_df: pd.DataFrame, curious_df: pd.DataFrame, failures: list[str]
) -> None:
    """Update records in REDCap."""
    # get updated records
    records = [stringify_secret_user_id(x) for x in curious_df["secretUserId"]]
    df_update_redcap = redcap_df.query(
        f'field_name == "mrn" and value in {records}'
    ).copy()[["record", "field_name", "value"]]

    # Set updated `enrollment_complete`
    df_update_redcap["field_name"] = "enrollment_complete"
    df_update_redcap["value"] = Values.PID625.enrollment_complete[
        "Parent and Participant information already sent to Curious"
    ]
    successes = set(
        redcap_df[
            (redcap_df["field_name"] == "mrn") & (~redcap_df["value"].isin(failures))
        ]["record"]
    )
    df_update_redcap = df_update_redcap[(df_update_redcap["record"].isin(successes))]

    try:
        rows_updated = redcap_api_push(
            df=df_update_redcap,
            token=_REDCAP_TOKENS.pid625,
            url=redcap_variables.Endpoints().base_url,
            headers=redcap_variables.headers,
        )
        logger.info(
            "%d rows successfully updated in PID %d.", rows_updated, _REDCAP_PID
        )
    except Exception:
        logger.exception("REDCap status update failed.")
        raise


def main() -> None:
    """Transfer data from REDCap to Curious."""
    # Initialize cache for minute-by-minute transfers (TTL: 2 minutes)
    cache = DataCache("redcap_to_curious", ttl_minutes=2)

    try:
        # get triggers from PID625
        data625 = fetch_data(
            _REDCAP_TOKENS.pid625,
            "mrn",
            Values.PID625.enrollment_complete.filter_logic("Ready to Send to Curious"),
        )
        mrn_filter_logic = " OR ".join([f"[mrn] = '{mrn}'" for mrn in data625["value"]])
        # get data from PID247
        data247 = fetch_data(
            _REDCAP_TOKENS.pid247,
            str(Fields.export_247.for_curious),
            mrn_filter_logic,
        )
        if data247.empty:
            logger.info(
                "REDCap PID 247: No participants marked 'Ready to Send to Curious'."
            )
            raise NoData
    except NoData:
        logger.info("No data to transfer from REDCap PID 247 to Curious.")
        return

    curious_data = format_redcap_data_for_curious(data247)

    if curious_data["child"].empty and curious_data["parent"].empty:
        logger.info("All participants already sent to Curious")
        return

    curious_endpoints = curious_variables.Endpoints()
    curious_credentials = curious_variables.AppletCredentials()
    failures = [
        *push_child_data(
            curious_data["child"], curious_endpoints, curious_credentials, cache
        ),
        *push_parent_data(curious_data, curious_endpoints, curious_credentials, cache),
    ]

    # Log cache statistics
    cache_stats = cache.get_stats()
    logger.info(
        "Cache statistics: %d entries, file size: %d bytes, last activity: %s",
        cache_stats["total_entries"],
        cache_stats["file_size_bytes"],
        cache_stats.get("last_activity", "never"),
    )

    update_redcap(data247, curious_data["child"], failures)


def push_child_data(
    curious_data: pd.DataFrame,
    curious_endpoints: curious_variables.Endpoints,
    curious_credentials: curious_variables.AppletCredentials,
    cache: DataCache,
) -> list[str]:
    """Push parent data to Curious."""
    applet_name = "CHILD-Healthy Brain Network Questionnaires"
    curious_tokens = curious_variables.Tokens(
        curious_endpoints, curious_credentials[applet_name]
    )
    child_data = curious_data.copy()
    child_data["accountType"] = "full"
    return send_to_curious(
        child_data,
        curious_tokens,
        curious_variables.applets[applet_name].applet_id,
        cache,
    )


def push_parent_data(
    curious_data: dict[Literal["child", "parent"], pd.DataFrame],
    curious_endpoints: curious_variables.Endpoints,
    curious_credentials: curious_variables.AppletCredentials,
    cache: DataCache,
) -> list[str]:
    """Push parent data to Curious."""
    applet_name = "Healthy Brain Network Questionnaires"
    curious_tokens = curious_variables.Tokens(
        curious_endpoints, curious_credentials[applet_name]
    )
    return [
        *send_to_curious(
            curious_data["child"],
            curious_tokens,
            curious_variables.applets[applet_name].applet_id,
            cache,
        ),
        *send_to_curious(
            curious_data["parent"],
            curious_tokens,
            curious_variables.applets[applet_name].applet_id,
            cache,
        ),
    ]


if __name__ == "__main__":
    main()
