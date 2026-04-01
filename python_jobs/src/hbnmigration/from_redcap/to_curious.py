"""
Transfer data from REDCap to Curious.

For each subject in PID 247, if `enrollment_complete` == 1,
prepares and copies the reviewed and approved participants by the RAs to Curious.
"""

import pandas as pd
import requests

from .._config_variables import curious_variables, redcap_variables
from ..exceptions import NoData
from ..from_curious.config import account_types, AccountType
from ..utility_functions import initialize_logging, new_curious_account, redcap_api_push
from .config import Fields, Values
from .from_redcap import fetch_data

logger = initialize_logging(__name__)

_REDCAP_TOKEN = redcap_variables.Tokens.pid247
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


def format_redcap_data_for_curious(redcap_data: pd.DataFrame) -> pd.DataFrame:
    """Format REDCap export data for Curious import."""
    curious_participant_data: list[pd.DataFrame] = []
    record_set: set[int | str] = set()
    for individual in ["parent", "child"]:
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
            df_pivoted["secretUserId"] = df_pivoted["secretUserId"].astype(str) + "_P"
        curious_participant_data.append(pd.DataFrame(df_pivoted[relevant_fields]))
    df_curious_participant_data = pd.concat(curious_participant_data).reset_index(
        drop=True
    )
    df_curious_participant_data = df_curious_participant_data.where(
        pd.notna(df_curious_participant_data), None
    )
    if "adult_enrollment_form_complete" in df_curious_participant_data.columns:
        # Check for `parent_involvement___1`
        df_curious_participant_data = pd.DataFrame(
            df_curious_participant_data[
                (df_curious_participant_data["parent_involvement"].apply(_in_set))
                | ~df_curious_participant_data["adult_enrollment_form_complete"]
            ]
        ).dropna(axis=1, how="all")

    # Now drop `parent_involvement` column before we push to Curious.
    df_curious_participant_data = df_curious_participant_data.drop(
        columns=["parent_involvement", "adult_enrollment_form_complete"],
        errors="ignore",
    )

    # Pad `secretUserId` with leading zeros to make it 5 characters long
    df_curious_participant_data["secretUserId"] = (
        df_curious_participant_data["secretUserId"].astype(str).str.zfill(5)
    )
    return df_curious_participant_data


def send_to_curious(
    df: pd.DataFrame, tokens: curious_variables.Tokens, applet_id: str
) -> list[str]:
    """Send new participants to Curious."""
    failures: list[str] = []
    headers = curious_variables.headers(tokens.access)

    # Loop through each REDCap transformed record and sent it to MindLogger
    for record in [
        {k: v for k, v in record.items() if v is not None}
        for record in df.to_dict(orient="records")
    ]:
        try:
            logger.info(
                "%s",
                new_curious_account(
                    tokens.endpoints.base_url, applet_id, record, headers
                ),
            )
        except requests.exceptions.RequestException:
            logger.exception("Error")
            failures.append(str(int(record["secretUserId"].rstrip("_P"))))
    return failures


def update_redcap(
    redcap_df: pd.DataFrame, curious_df: pd.DataFrame, failures: list[str]
) -> None:
    """Update records in REDCap."""
    # get updated records
    records = [
        str(int(x)) for x in curious_df["secretUserId"] if not str(x).endswith("_P")
    ]
    df_update_redcap = redcap_df.query(
        f'field_name == "mrn" and value in {records}'
    ).copy()[["record", "field_name", "value"]]

    # Set updated `enrollment_complete`
    df_update_redcap["field_name"] = "enrollment_complete"
    df_update_redcap["value"] = Values.PID247.enrollment_complete[
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
            token=_REDCAP_TOKEN,
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
    try:
        # get data from PID247
        data247 = fetch_data(
            _REDCAP_TOKEN,
            str(Fields.export_247.for_curious),
            Values.PID247.enrollment_complete.filter_logic("Ready to Send to Curious"),
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
    for account_type in account_types:
        _check_for_data_to_process(curious_data, account_type)
    curious_endpoints = curious_variables.Endpoints()
    curious_tokens = curious_variables.Tokens(
        curious_endpoints, curious_variables.Credentials.hbn_mindlogger
    )
    failures = send_to_curious(
        curious_data,
        curious_tokens,
        curious_variables.applet_ids["Healthy Brain Network Questionnaires"],
    )
    update_redcap(data247, curious_data, failures)


if __name__ == "__main__":
    main()
