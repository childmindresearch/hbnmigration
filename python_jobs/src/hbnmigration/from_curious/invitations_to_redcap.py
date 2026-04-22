"""Monitor Curious account invitations and send updates to REDCap."""

from io import StringIO
import logging
from typing import Literal, Optional

import polars as pl
import requests

from mindlogger_data_export.mindlogger import MindloggerData
from mindlogger_data_export.outputs import NamedOutput, RedcapImportFormat

from .._config_variables import curious_variables, redcap_variables
from ..from_redcap.config import Fields as RedcapFields, Values as RedcapValues
from ..utility_functions import (
    CuriousDecryptedAnswer,
    CuriousId,
    DataCache,
    fetch_api_data,
    initialize_logging,
    yesterday_or_more_recent,
)
from .config import curious_authenticate, invitation_statuses, Values as CuriousValues
from .decryption import decrypt_single, get_applet_encryption
from .utils import deduplicate_dataframe

initialize_logging()
logger = logging.getLogger(__name__)

# Type for distinguishing account types
AccountContext = Literal["responder", "child"]

# REDCap token - will be fetched based on PID
REDCAP_TOKEN: str = ""


class Endpoints:
    """Initialized endpoints."""

    Curious = curious_variables.Endpoints()
    """Curious endpoints."""
    Redcap = redcap_variables.Endpoints()
    """REDCap endpoints."""


def lookup_mrn_from_r_id(r_id: str, token: str) -> str | None:
    """
    Look up MRN for a given r_id from REDCap PID 625.

    Parameters
    ----------
    r_id : str
        The respondent secret ID from Curious
    token : str
        REDCap API token

    Returns
    -------
    str | None
        The MRN if found, None otherwise

    """
    try:
        # Fetch r_id and mrn fields from REDCap
        data = fetch_api_data(
            Endpoints.Redcap.base_url,
            redcap_variables.headers,
            {
                "token": token,
                "content": "record",
                "format": "csv",
                "type": "flat",
                "fields": str(RedcapFields.export_operations.for_mrn_lookup),
                "rawOrLabel": "raw",
                "rawOrLabelHeaders": "raw",
                "exportCheckboxLabel": "false",
                "exportSurveyFields": "false",
                "exportDataAccessGroups": "false",
                "returnFormat": "csv",
            },
        )

        if data.empty:
            logger.warning("No data returned from REDCap for MRN lookup")
            return None

        # Filter for matching r_id
        matching = data[data["r_id"].astype(str) == str(r_id)]

        if matching.empty:
            logger.debug("No MRN found for r_id: %s", r_id)
            return None

        # Get the MRN (should be same across all rows for this r_id)
        mrn = matching["record_id"].iloc[0]
        logger.debug("Found MRN %s for r_id %s", mrn, r_id)
        return str(mrn)

    except Exception as e:
        logger.warning("Error looking up MRN for r_id %s: %s", r_id, e)
        return None


def check_activity_response(
    token: str,
    respondent: dict,
    applet_id: CuriousId,
    activity_id: CuriousId,
    account_context: AccountContext,
) -> list[NamedOutput]:
    """
    Check for response to activity.

    Parameters
    ----------
    token : str
        Curious API token
    respondent : dict
        Respondent record with account information
    applet_id : CuriousId
        Applet ID to check
    activity_id : CuriousId
        Activity ID to check
    account_context : AccountContext
        Whether checking "responder" or "child" account

    Returns
    -------
    list[NamedOutput]
        Formatted outputs for REDCap import

    """
    encryption = get_applet_encryption(Endpoints.Curious.applet(applet_id), token)
    response = requests.get(
        Endpoints.Curious.applet_activity_answers_list(applet_id, activity_id)
        + f"?targetSubjectId={respondent['respondent_id']}",
        headers=curious_variables.headers(token),
    )
    all_formatted_data: list[NamedOutput] = []
    if response.status_code == requests.codes["okay"]:
        result = response.json()["result"]
        if result:
            applet_name = respondent.get(
                "applet_name", "Healthy Brain Network Questionnaires"
            )
            for answer in result:
                decrypted_answer = decrypt_single(
                    answer,
                    encryption,
                    curious_variables.AppletCredentials()[applet_name][
                        "applet_password"
                    ],
                )
                formatted_data = format_for_redcap(
                    decrypted_answer, respondent, account_context
                )
                if formatted_data:
                    all_formatted_data.extend(formatted_data)
    return all_formatted_data


def check_activity_responses(
    token: str,
    df: pl.DataFrame,
    applet_id: CuriousId,
    activity_id: CuriousId,
    account_context: AccountContext,
) -> pl.DataFrame:
    """
    Check for responses to activity for all records.

    Parameters
    ----------
    token : str
        Curious API token
    df : pl.DataFrame
        DataFrame with respondent records
    applet_id : CuriousId
        Applet ID to check
    activity_id : CuriousId
        Activity ID to check
    account_context : AccountContext
        Whether checking "responder" or "child" account

    Returns
    -------
    pl.DataFrame
        Combined DataFrame with all responses

    """
    responses = []
    for row in df.iter_rows(named=True):
        response = check_activity_response(
            token, row, applet_id, activity_id, account_context
        )
        responses += [r.output for r in response]
    return pl.concat(responses) if responses else df


def create_invitation_record(
    respondent: dict,
    applet_id: CuriousId,
    account_context: AccountContext,
    redcap_token: str,
) -> dict | None:
    """
    Create a dictionary for a respondent with MRN and invitation status.

    Parameters
    ----------
    respondent : dict
        Respondent data from Curious API
    applet_id : CuriousId
        Applet ID
    account_context : AccountContext
        Whether this is "responder" or "child" account
    redcap_token : str
        REDCap API token for MRN lookup

    Returns
    -------
    dict | None
        Record dict for REDCap import, or None if invalid

    """
    details: list[dict] = [
        detail for detail in respondent["details"] if detail["appletId"] == applet_id
    ]
    if not details:
        return None

    detail = details[-1]
    secret_id: str = detail["respondentSecretId"]

    # Normalize secret_id
    try:
        secret_id = str(int(secret_id))
    except ValueError:
        secret_id = str(secret_id)

    # Determine instrument name and event based on account context
    if account_context == "responder":
        instrument = "curious_account_created_responder"
        event_name = "admin_arm_1"
        # For responders, look up MRN from r_id
        mrn = lookup_mrn_from_r_id(secret_id, redcap_token)
        if not mrn:
            logger.warning(
                "Could not find MRN for responder r_id: %s, skipping", secret_id
            )
            return None
        record_id = mrn
        source_secret_id = secret_id  # Store the r_id
    else:  # child
        instrument = "curious_account_created_child"
        event_name = "admin_arm_1"
        # For children, the secret_id should be the MRN
        record_id = secret_id
        source_secret_id = secret_id

    # Add field suffix for child accounts
    # Note: The base field name is always "curious_account_created_"
    # For child, we add "_c" suffix to the field name itself
    field_suffix = "_c" if account_context == "child" else ""

    return {
        "record_id": record_id,
        f"curious_account_created_source_secret_id{field_suffix}": source_secret_id,
        f"curious_account_created_invite_status{field_suffix}": invitation_statuses[
            respondent["status"]
        ],
        "redcap_event_name": event_name,
        f"{instrument}_"
        "complete": RedcapValues.PID625.curious_account_created_responder_complete[
            "Incomplete"
        ]
        if account_context == "responder"
        else RedcapValues.PID625.curious_account_created_child_complete["Incomplete"],
        # Note: respondent_id is NOT sent to REDCap, only used internally
        "respondent_id": detail["subjectId"],
        "instrument": instrument,
        "account_context": account_context,
    }


def format_for_redcap(
    ml_data: CuriousDecryptedAnswer,
    redcap_context: dict,
    account_context: AccountContext,
) -> list[NamedOutput]:
    """
    Format response data for REDCap import.

    Parameters
    ----------
    ml_data : CuriousDecryptedAnswer
        Decrypted Curious response data
    redcap_context : dict
        Context with record_id, event, etc.
    account_context : AccountContext
        Whether this is "responder" or "child" account

    Returns
    -------
    list[NamedOutput]
        Formatted outputs for REDCap import

    """
    if not ml_data:
        return []

    # Extract REDCap fields from context
    record_id = redcap_context["record_id"]
    redcap_event_name: str = redcap_context["redcap_event_name"]
    instrument = redcap_context["instrument"]

    # Build DataFrame directly from structured data
    rows = []
    submit_id = ml_data.get("submitId", instrument)
    start_time = ml_data["startDatetime"]
    end_time = ml_data["endDatetime"]

    for idx, item in enumerate(ml_data.get("items", [])):
        answer_value = ml_data["answer"][idx] if idx < len(ml_data["answer"]) else {}

        # Extract response options
        response_options = []
        if "responseValues" in item and "options" in item["responseValues"]:
            for opt in item["responseValues"]["options"]:
                response_options.append(
                    {
                        "name": opt.get("text", ""),
                        "value": opt.get("value", 0),
                        "score": opt.get("score", 0)
                        if opt.get("score") is not None
                        else 0,
                    }
                )

        # Build response value based on item type
        response_val = answer_value.get("value")
        response_value = {
            "type": item["responseType"],
            "raw_value": str(response_val) if response_val is not None else None,
            "null_value": response_val is None,
            "single_value": response_val if isinstance(response_val, int) else None,
            "value": response_val if isinstance(response_val, list) else None,
            "text": response_val if isinstance(response_val, str) else None,
            "file": None,
            "date": None,
            "time": None,
            "time_range": None,
            "geo": None,
            "matrix": None,
            "optional_text": None,
            "subscale": None,
        }

        row = {
            "item_id": item["id"],
            "item_name": item["name"],
            "item_prompt": item["question"].get("en", ""),
            "item_type": item["responseType"],
            "item_response_options": response_options,
            "response_value": response_value,
            "response_status": "completed",
            "response_raw_score": None,
            "activity_start_time": start_time,
            "activity_end_time": end_time,
            "utc_timezone_offset": 0,
            "applet_version": ml_data.get("version", ""),
            "target_user_secret_id": ml_data.get("respondentSecretId", ""),
            "source_user_secret_id": ml_data.get("sourceSecretId", ""),
            "activity_submission_id": submit_id,
            "activity_flow_submission_id": submit_id,
        }
        rows.append(row)

    # Create DataFrame with proper structure for formatter
    df = pl.DataFrame(rows).with_columns(
        pl.col("activity_start_time").alias("activity_start_time_str"),
        pl.col("activity_end_time").alias("activity_end_time_str"),
        pl.from_epoch(
            pl.col("activity_start_time")
            .str.strptime(pl.Datetime("ms"), "%Y-%m-%dT%H:%M:%S%.3fZ")
            .dt.epoch("ms"),
            time_unit="ms",
        )
        .dt.replace_time_zone("UTC")
        .alias("activity_start_time"),
        pl.from_epoch(
            pl.col("activity_end_time")
            .str.strptime(pl.Datetime("ms"), "%Y-%m-%dT%H:%M:%S%.3fZ")
            .dt.epoch("ms"),
            time_unit="ms",
        )
        .dt.replace_time_zone("UTC")
        .alias("activity_end_time"),
        pl.duration(milliseconds=pl.col("utc_timezone_offset")).alias(
            "utc_timezone_offset"
        ),
        pl.struct(
            pl.lit(instrument).alias("id"),
            pl.lit(instrument.replace("_", " ").title()).alias("name"),
        ).alias("activity"),
        pl.struct(
            pl.lit(instrument).alias("id"),
            pl.lit(instrument.replace("_", " ").title()).alias("name"),
            pl.col("activity_flow_submission_id").alias("submission_id"),
        ).alias("activity_flow"),
        pl.struct(
            pl.col("activity_submission_id").alias("id"),
            pl.lit(None).cast(pl.String).alias("review_id"),
        ).alias("activity_submission"),
        pl.struct(
            pl.lit(None).cast(pl.String).alias("id"),
            pl.lit(None).cast(pl.String).alias("history_id"),
            pl.lit(None).cast(pl.Datetime("ms", "UTC")).alias("start_time"),
        ).alias("activity_schedule"),
        pl.struct(
            pl.col("item_id").alias("id"),
            pl.col("item_name").alias("name"),
            pl.col("item_prompt").alias("prompt"),
            pl.col("item_type").alias("type"),
            pl.lit(None).cast(pl.String).alias("raw_options"),
            pl.col("item_response_options").alias("response_options"),
        ).alias("item"),
        pl.struct(
            pl.col("response_status").alias("status"),
            pl.col("response_raw_score").alias("raw_score"),
            pl.lit(None).cast(pl.String).alias("raw_response"),
            pl.col("response_value").alias("value"),
        ).alias("response"),
        pl.struct(
            pl.col("target_user_secret_id").alias("secret_id"),
            pl.lit(None).cast(pl.String).alias("id"),
            pl.lit(None).cast(pl.String).alias("nickname"),
            pl.lit(None).cast(pl.String).alias("relation"),
            pl.lit(None).cast(pl.String).alias("tag"),
        ).alias("target_user"),
        pl.struct(
            pl.col("source_user_secret_id").alias("secret_id"),
            pl.lit(None).cast(pl.String).alias("id"),
            pl.lit(None).cast(pl.String).alias("nickname"),
            pl.lit(None).cast(pl.String).alias("relation"),
            pl.lit(None).cast(pl.String).alias("tag"),
        ).alias("source_user"),
        pl.struct(
            pl.lit(None).cast(pl.String).alias("secret_id"),
            pl.lit(None).cast(pl.String).alias("id"),
            pl.lit(None).cast(pl.String).alias("nickname"),
            pl.lit(None).cast(pl.String).alias("relation"),
            pl.lit(None).cast(pl.String).alias("tag"),
        ).alias("input_user"),
        pl.struct(
            pl.lit(None).cast(pl.String).alias("secret_id"),
            pl.lit(None).cast(pl.String).alias("id"),
            pl.lit(None).cast(pl.String).alias("nickname"),
            pl.lit(None).cast(pl.String).alias("relation"),
            pl.lit(None).cast(pl.String).alias("tag"),
        ).alias("account_user"),
    )

    # Create activity_time struct
    df = df.with_columns(
        pl.struct(
            pl.col("activity_start_time_str")
            .str.strptime(pl.Datetime("ms"), "%Y-%m-%dT%H:%M:%S%.3fZ")
            .alias("start_time"),
            pl.col("activity_end_time_str")
            .str.strptime(pl.Datetime("ms"), "%Y-%m-%dT%H:%M:%S%.3fZ")
            .alias("end_time"),
        ).alias("activity_time"),
    )

    # Drop intermediate columns
    columns_to_drop = [
        col
        for col in [
            "item_id",
            "item_name",
            "item_prompt",
            "item_type",
            "item_response_options",
            "response_value",
            "response_status",
            "response_raw_score",
            "target_user_secret_id",
            "source_user_secret_id",
            "activity_submission_id",
            "activity_flow_submission_id",
            "activity_start_time",
            "activity_end_time",
            "activity_start_time_str",
            "activity_end_time_str",
            "activity_time_start_time",
            "activity_time_end_time",
        ]
        if col in df.columns
    ]
    df = df.drop(columns_to_drop)

    # Format for REDCap
    formatter = RedcapImportFormat(project={instrument: redcap_event_name})
    results = formatter.produce(MindloggerData(df))

    # Add suffix for child instruments to avoid naming conflicts
    field_suffix = "_c" if account_context == "child" else ""

    for result in results:
        # Rename columns to match instrument
        result.output = result.output.rename(
            lambda col: (
                col.replace("curiousaccountcreated", instrument, 1)
                if col.startswith("curiousaccountcreated")
                else col
            )
        )

        # Add suffix for child fields if needed (only data fields, not complete)
        if field_suffix:
            result.output = result.output.rename(
                lambda col: (
                    f"{col}{field_suffix}"
                    if col.startswith("curious_account_created_")
                    and not col.endswith("_complete")
                    and col not in ["record_id", "redcap_event_name"]
                    else col
                )
            )

        # Add context columns
        context_columns = [
            pl.lit(record_id).alias("record_id"),
            pl.lit(redcap_event_name).alias("redcap_event_name"),
        ]

        # Map short keys to full field names with suffix
        field_mapping = {
            "source_secret_id": f"{instrument}_source_secret_id{field_suffix}",
            "invite_status": f"{instrument}_invite_status"
            f"{field_suffix if account_context == 'child' else ''}",
        }

        for short_key, full_key in field_mapping.items():
            if short_key in redcap_context:
                context_columns.append(
                    pl.lit(redcap_context[short_key]).alias(full_key)
                )

        # Determine complete status - LEAVE INCOMPLETE per new business logic
        response_field = f"{instrument}_account_created_response{field_suffix}"
        complete_field = f"{instrument}_complete"

        if response_field in result.output.columns:
            # Check if they confirmed account creation
            # Both use the same response field descriptor
            hbnq = CuriousValues.HealthyBrainNetworkQuestionnaires
            curious_values = hbnq.CuriousAccountCreated.acount_created
            context_columns.append(
                pl.when(
                    pl.col(response_field).cast(pl.Utf8)
                    == curious_values["I confirm that I have created a Curious account"]
                )
                .then(
                    pl.lit(
                        RedcapValues.PID625.curious_account_created_responder_complete[
                            "Unverified"
                        ]
                    )
                )
                .otherwise(
                    pl.lit(
                        RedcapValues.PID625.curious_account_created_responder_complete[
                            "Incomplete"
                        ]
                    )
                )
                .alias(complete_field)
            )
        elif redcap_context.get("invite_status") == "3":
            # Invite accepted but no response yet
            context_columns.append(
                pl.lit(
                    RedcapValues.PID625.curious_account_created_responder_complete[
                        "Unverified"
                    ]
                ).alias(complete_field)
            )
        else:
            # Invite not sent or not accepted
            context_columns.append(
                pl.lit(
                    RedcapValues.PID625.curious_account_created_responder_complete[
                        "Incomplete"
                    ]
                ).alias(complete_field)
            )

        result.output = result.output.with_columns(context_columns)

    return results


def pull_data_from_curious(
    token: str,
    applet_name: str,
    account_context: AccountContext,
    redcap_token: str,
) -> pl.DataFrame:
    """
    Pull data from Curious and construct a Polars DataFrame.

    Parameters
    ----------
    token : str
        Curious API token
    applet_name : str
        Name of the applet to query
    account_context : AccountContext
        Whether pulling "responder" or "child" accounts
    redcap_token : str
        REDCap API token for MRN lookup

    Returns
    -------
    pl.DataFrame
        DataFrame with invitation records

    """
    applet_id = curious_variables.applets[applet_name].applet_id
    owner_id = curious_variables.owner_ids.get(
        "Healthy Brain Network (HBN)", next(iter(curious_variables.owner_ids.values()))
    )

    response = requests.get(
        Endpoints.Curious.invitation_statuses(owner_id, applet_id),
        headers=curious_variables.headers(token),
    )
    response.raise_for_status()

    records = []
    for respondent in response.json().get("result", []):
        last_seen = respondent.get("lastSeen")
        if last_seen is None or yesterday_or_more_recent(last_seen):
            # Add applet name to context
            respondent["applet_name"] = applet_name
            record = create_invitation_record(
                respondent, applet_id, account_context, redcap_token
            )
            if record is not None:
                records.append(record)

    invitation_df = pl.DataFrame(records)
    if not invitation_df.is_empty():
        invitation_df = update_already_completed(
            invitation_df, account_context, redcap_token
        )

    return invitation_df


def push_to_redcap(
    data: pl.DataFrame | str,
    token: str,
    cache: DataCache | None = None,
) -> int:
    """
    Push data to REDCap with deduplication.

    Parameters
    ----------
    data : pl.DataFrame | str
        DataFrame or CSV string to upload
    token : str
        REDCap API token
    cache : DataCache | None, optional
        Cache for tracking processed records

    Returns
    -------
    int
        Number of records updated

    """
    # Handle both DataFrame and CSV string inputs for backward compatibility
    if isinstance(data, str):
        df = pl.read_csv(StringIO(data))
    else:
        df = data

    # Determine instrument from data
    instrument = (
        df.select("instrument").to_series()[0] if "instrument" in df.columns else None
    )
    if not instrument:
        logger.warning("Could not determine instrument from data")
        instrument = "curious_account_created_responder"  # Default

    # Deduplicate before pushing
    df, num_duplicates = deduplicate_dataframe(
        df,
        token,
        Endpoints.Redcap.base_url,
        redcap_variables.headers,
        instrument,
    )

    if df.is_empty():
        logger.info("All invitation rows are duplicates, skipping upload")
        return 0

    if num_duplicates > 0:
        logger.info("Removed %d duplicate invitation rows before push", num_duplicates)

    # Remove metadata columns before upload
    columns_to_drop = ["instrument", "account_context", "respondent_id"]
    df = df.drop([col for col in columns_to_drop if col in df.columns])

    csv_data = df.write_csv()
    push_data = {
        "token": token,
        "content": "record",
        "action": "import",
        "format": "csv",
        "type": "flat",
        "overwriteBehavior": "normal",
        "forceAutoNumber": "false",
        "data": csv_data,
        "returnContent": "count",
        "returnFormat": "csv",
    }

    r = requests.post(Endpoints.Redcap.base_url, data=push_data)
    if r.status_code != requests.codes["okay"]:
        logger.exception("%s\n%s\nHTTP Status: %d", r.reason, r.text, r.status_code)
    r.raise_for_status()
    return r.json()


def update_already_completed(
    df: pl.DataFrame, account_context: AccountContext, token: str
) -> pl.DataFrame:
    """
    For any already-completed records in REDCap, filter them out.

    Parameters
    ----------
    df : pl.DataFrame
        DataFrame with invitation records
    account_context : AccountContext
        Whether checking "responder" or "child" accounts
    token : str
        REDCap API token

    Returns
    -------
    pl.DataFrame
        Filtered DataFrame

    """
    instrument = (
        "curious_account_created_responder"
        if account_context == "responder"
        else "curious_account_created_child"
    )
    complete_field = f"{instrument}_complete"

    already_completed = fetch_api_data(
        Endpoints.Redcap.base_url,
        redcap_variables.headers,
        {
            "token": token,
            "content": "record",
            "action": "export",
            "format": "csv",
            "type": "eav",
            "csvDelimiter": "",
            "fields": complete_field,
            "filterLogic": (
                RedcapValues.PID625.curious_account_created_responder_complete.filter_logic(
                    "Complete"
                )
            ),
            "rawOrLabel": "raw",
            "rawOrLabelHeaders": "raw",
            "exportCheckboxLabel": "false",
            "exportSurveyFields": "false",
            "exportDataAccessGroups": "false",
            "returnFormat": "csv",
        },
        return_type=list,
    )

    return df.filter(~pl.col("record_id").is_in(already_completed)).drop_nulls()


def main(  # noqa: PLR0912,PLR0915
    applet_name: Optional[str] = None,
    target_pid: Literal[625, 891] = 625,
    account_context: Optional[AccountContext] = None,
) -> None:
    """
    Monitor Curious account invitations and send updates to REDCap.

    Parameters
    ----------
    applet_name : str, optional
        Name of Curious applet to monitor
    target_pid : Literal[625, 891], optional
        Target REDCap project ID (default: 625)
    account_context : AccountContext, optional
        Whether to process "responder" or "child" accounts.
        If None, processes both.

    """
    # Feature flag for PID 891
    if target_pid == 891:  # noqa: PLR2004
        try:
            token = redcap_variables.Tokens().pid891
            if token is None:
                msg = "PID 891 token is None"
                raise AttributeError(msg)
        except AttributeError:
            logger.warning(
                "PID 891 not yet configured, skipping. "
                "This is expected during transition period."
            )
            return
    else:
        token = redcap_variables.Tokens().pid625

    # Set global token for MRN lookups (always use 625 for lookups)
    lookup_token = redcap_variables.Tokens().pid625

    cache = DataCache(f"curious_invitations_to_redcap_{target_pid}", ttl_minutes=2)

    # Process responder accounts
    if account_context is None or account_context == "responder":
        responder_applet = applet_name or "Healthy Brain Network Questionnaires"

        try:
            auth = curious_authenticate(responder_applet)
        except (KeyError, ConnectionError) as e:
            logger.warning(
                "Responder applet not configured: %s. Skipping responder processing.",
                e,
            )
        else:
            invitation_df_responder = pull_data_from_curious(
                auth.access, responder_applet, "responder", lookup_token
            )

            if not invitation_df_responder.is_empty():
                # Cache filtering
                if cache:
                    unprocessed_records = cache.get_unprocessed_records(
                        invitation_df_responder["record_id"].to_list()
                    )
                    if len(unprocessed_records) < len(invitation_df_responder):
                        logger.info(
                            "Skipping %d already-processed responder records "
                            "(cache hit)",
                            len(invitation_df_responder) - len(unprocessed_records),
                        )
                        invitation_df_responder = invitation_df_responder.filter(
                            pl.col("record_id").is_in(unprocessed_records)
                        )

                if not invitation_df_responder.is_empty():
                    invitation_df_responder = check_activity_responses(
                        auth.access,
                        invitation_df_responder,
                        curious_variables.applets[responder_applet].applet_id,
                        curious_variables.applets[responder_applet]
                        .activities["Curious Account Created"]
                        .activity_id,
                        "responder",
                    ).unique(subset=["record_id"], keep="last")

                    n_records_responder = push_to_redcap(
                        invitation_df_responder, token, cache
                    )
                    logger.info(
                        "%d responder records updated in REDCap (PID %d)",
                        n_records_responder,
                        target_pid,
                    )

                    if cache and n_records_responder > 0:
                        cache.bulk_mark_processed(
                            invitation_df_responder["record_id"].to_list(),
                            metadata={
                                "count": n_records_responder,
                                "type": "responder",
                            },
                        )
                else:
                    logger.info("All responder invitations already processed in cache.")
            else:
                logger.info("No responder invitations to update.")

    # Process child accounts
    if account_context is None or account_context == "child":
        child_applet = "CHILD-Healthy Brain Network Questionnaires"

        try:
            auth_child = curious_authenticate(child_applet)
        except (KeyError, ConnectionError) as e:
            logger.warning(
                "Child applet not yet configured: %s. "
                "Skipping child account processing.",
                e,
            )
        else:
            invitation_df_child = pull_data_from_curious(
                auth_child.access, child_applet, "child", lookup_token
            )

            if not invitation_df_child.is_empty():
                # Cache filtering
                if cache:
                    unprocessed_records = cache.get_unprocessed_records(
                        invitation_df_child["record_id"].to_list()
                    )
                    if len(unprocessed_records) < len(invitation_df_child):
                        logger.info(
                            "Skipping %d already-processed child records (cache hit)",
                            len(invitation_df_child) - len(unprocessed_records),
                        )
                        invitation_df_child = invitation_df_child.filter(
                            pl.col("record_id").is_in(unprocessed_records)
                        )

                if not invitation_df_child.is_empty():
                    invitation_df_child = check_activity_responses(
                        auth_child.access,
                        invitation_df_child,
                        curious_variables.applets[child_applet].applet_id,
                        curious_variables.applets[child_applet]
                        .activities["Curious Account Created"]
                        .activity_id,
                        "child",
                    ).unique(subset=["record_id"], keep="last")

                    n_records_child = push_to_redcap(invitation_df_child, token, cache)
                    logger.info(
                        "%d child records updated in REDCap (PID %d)",
                        n_records_child,
                        target_pid,
                    )

                    if cache and n_records_child > 0:
                        cache.bulk_mark_processed(
                            invitation_df_child["record_id"].to_list(),
                            metadata={"count": n_records_child, "type": "child"},
                        )
                else:
                    logger.info("All child invitations already processed in cache.")
            else:
                logger.info("No child invitations to update.")

    # Log cache statistics
    cache_stats = cache.get_stats()
    logger.info(
        "Cache statistics: %d entries, file size: %d bytes, last activity: %s",
        cache_stats["total_entries"],
        cache_stats["file_size_bytes"],
        cache_stats.get("last_activity", "never"),
    )


if __name__ == "__main__":
    main()
