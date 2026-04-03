"""Monitor Curious account invitations and send updates to REDCap."""

import logging

import polars as pl
import requests

from mindlogger_data_export.mindlogger import MindloggerData
from mindlogger_data_export.outputs import NamedOutput, RedcapImportFormat

from .._config_variables import curious_variables, redcap_variables
from ..from_redcap.config import Values as RedcapValues
from ..utility_functions import (
    CuriousDecryptedAnswer,
    CuriousId,
    fetch_api_data,
    initialize_logging,
    ProjectStatus,
    yesterday_or_more_recent,
)
from .config import curious_authenticate, invitation_statuses
from .decryption import decrypt_single, get_applet_encryption

initialize_logging()
logger = logging.getLogger(__name__)
PROJECT_STATUS: ProjectStatus = "dev"


class Endpoints:
    """Initialized endpoints."""

    Curious = curious_variables.Endpoints()
    """Curious endpoints."""
    Redcap = redcap_variables.Endpoints()
    """REDCap endpoints."""


def check_activity_response(
    token, respondent: dict, applet_id: CuriousId, activity_id: CuriousId
) -> list[NamedOutput]:
    """Check for response to activity."""
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
            for answer in result:
                decrypted_answer = decrypt_single(
                    answer,
                    encryption,
                    curious_variables.AppletCredentials.hbn_mindlogger[
                        "Healthy Brain Network Questionnaires"
                    ]["applet_password"],
                )
                formatted_data = format_for_redcap(decrypted_answer, respondent)
                if formatted_data:
                    all_formatted_data.extend(formatted_data)
    return all_formatted_data


def check_activity_responses(
    token: str, df: pl.DataFrame, applet_id: CuriousId, activity_id: CuriousId
) -> pl.DataFrame:
    """Check for responses to activity."""
    responses = []
    for row in df.iter_rows(named=True):
        response = check_activity_response(token, row, applet_id, activity_id)
        responses += [r.output for r in response]
    return pl.concat(responses) if responses else df


def create_invitation_record(respondent: dict, applet_id: CuriousId) -> dict | None:
    """Create a dictionary for a respondent with MRN and invitation status."""
    details: list[dict] = [
        detail for detail in respondent["details"] if detail["appletId"] == applet_id
    ]
    if not details:
        return None
    detail = details[-1]
    secret_id: str = detail["respondentSecretId"]
    try:
        secret_id = str(int(secret_id))
    except ValueError:
        secret_id = str(secret_id)
    if not secret_id.endswith("_P"):
        return None
    return {
        "record_id": secret_id[:-2],
        "source_secret_id": secret_id,
        "invite_status": invitation_statuses[respondent["status"]],
        "redcap_event_name": "curious_parent_arm_1",
        "complete": RedcapValues.PID744.curious_account_created_complete["Incomplete"],
        "respondent_id": detail["subjectId"],
    }


def format_for_redcap(
    ml_data: CuriousDecryptedAnswer, redcap_context: dict
) -> list[NamedOutput]:
    """Format response data for REDCap import."""
    if not ml_data:
        return []

    # Extract REDCap fields from context
    record_id = redcap_context["record_id"]
    redcap_event_name: str = redcap_context["redcap_event_name"]

    # Build DataFrame directly from structured data
    rows = []
    submit_id = ml_data.get("submitId", "curious_account_created")
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

    df = pl.DataFrame(rows).with_columns(
        pl.from_epoch(
            pl.col("activity_start_time")
            .str.strptime(pl.Datetime("ms"), "%Y-%m-%dT%H:%M:%S%.f")
            .dt.epoch("ms"),
            time_unit="ms",
        )
        .dt.replace_time_zone("UTC")
        .alias("activity_start_time"),
        pl.from_epoch(
            pl.col("activity_end_time")
            .str.strptime(pl.Datetime("ms"), "%Y-%m-%dT%H:%M:%S%.f")
            .dt.epoch("ms"),
            time_unit="ms",
        )
        .dt.replace_time_zone("UTC")
        .alias("activity_end_time"),
        pl.duration(milliseconds=pl.col("utc_timezone_offset")).alias(
            "utc_timezone_offset"
        ),
        pl.struct(
            pl.lit("curious_account_created").alias("id"),
            pl.lit("Curious Account Created").alias("name"),
        ).alias("activity"),
        pl.struct(
            pl.lit("curious_account_created").alias("id"),
            pl.lit("Curious Account Created").alias("name"),
            pl.col("activity_flow_submission_id").alias("submission_id"),
        ).alias("activity_flow"),
        pl.struct(
            pl.col("activity_submission_id").alias("id"),
            pl.lit(None).cast(pl.String).alias("review_id"),
        ).alias("activity_submission"),
        pl.struct(
            pl.col("activity_start_time").alias("start_time"),
            pl.col("activity_end_time").alias("end_time"),
        ).alias("activity_time"),
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

    # Only drop columns that exist
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
        ]
        if col in df.columns
    ]
    df = df.drop(columns_to_drop)

    formatter = RedcapImportFormat(
        project={"curious_account_created": redcap_event_name}
    )
    results = formatter.produce(MindloggerData(df))

    for result in results:
        result.output = result.output.rename(
            lambda col: (
                col.replace("curiousaccountcreated_", "curious_account_created_", 1)
                if col.startswith("curiousaccountcreated_")
                else col
            )
        )
        context_columns = [
            pl.lit(record_id).alias("record_id"),
            pl.lit(redcap_event_name).alias("redcap_event_name"),
        ]

        field_mapping = {
            "source_secret_id": "curious_account_created_source_secret_id",
            "invite_status": "curious_account_created_invite_status",
        }

        for short_key, full_key in field_mapping.items():
            if short_key in redcap_context:
                context_columns.append(
                    pl.lit(redcap_context[short_key]).alias(full_key)
                )
        if "curious_account_created_account_created_response" in result.output.columns:
            curious_account_created_account_created_response = (
                RedcapValues.PID744.curious_account_created_account_created_response
            )
            context_columns.append(
                pl.when(
                    pl.col("curious_account_created_account_created_response").cast(
                        pl.Utf8
                    )
                    == curious_account_created_account_created_response[
                        "I confirm that I have created a Curious account"
                    ]
                )
                .then(
                    pl.lit(
                        RedcapValues.PID744.curious_account_created_complete["Complete"]
                    )
                )
                .otherwise(
                    pl.lit(
                        RedcapValues.PID744.curious_account_created_complete[
                            "Unverified"
                        ]
                    )
                )
                .alias("curious_account_created_complete")
            )
        # Column doesn't exist - check invite_status
        elif redcap_context.get("invite_status") == "3":
            context_columns.append(
                pl.lit(
                    RedcapValues.PID744.curious_account_created_complete["Unverified"]
                ).alias("curious_account_created_complete")
            )
        else:
            context_columns.append(
                pl.lit(
                    RedcapValues.PID744.curious_account_created_complete["Incomplete"]
                ).alias("curious_account_created_complete")
            )
        result.output = result.output.with_columns(context_columns)

    return results


def pull_data_from_curious(token: str) -> pl.DataFrame:
    """Pull data from Curious and construct a Polars DataFrame."""
    response = requests.get(
        Endpoints.Curious.invitation_statuses(
            curious_variables.owner_ids["Healthy Brain Network (HBN)"],
            curious_variables.applet_ids["Healthy Brain Network Questionnaires"],
        ),
        headers=curious_variables.headers(token),
    )
    response.raise_for_status()
    applet_id = curious_variables.applet_ids["Healthy Brain Network Questionnaires"]
    records = []
    for respondent in response.json().get("result", []):
        last_seen = respondent.get("lastSeen")

        if last_seen is None or yesterday_or_more_recent(last_seen):
            record = create_invitation_record(respondent, applet_id)
            if record is not None:
                records.append(record)
    invitation_df = pl.DataFrame(records)
    if not invitation_df.is_empty():
        invitation_df = update_already_completed(invitation_df)
    return invitation_df


def push_to_redcap(csv_data: str) -> int:
    """
    Push data to RedCap.

    Returns
    -------
    int
        number of records updated

    """
    data = {
        "token": redcap_variables.Tokens.pid744
        if PROJECT_STATUS == "dev"
        else redcap_variables.Tokens.pid625,
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
    r = requests.post(Endpoints.Redcap.base_url, data=data)
    if r.status_code != requests.codes["okay"]:
        logger.exception("%s\n%s\nHTTP Status: %d", r.reason, r.text, r.status_code)
    r.raise_for_status()
    return r.json()


def update_already_completed(df: pl.DataFrame) -> pl.DataFrame:
    """For any already-completed records in REDCap, filter them out."""
    already_completed = fetch_api_data(
        Endpoints.Redcap.base_url,
        redcap_variables.headers,
        {
            "token": redcap_variables.Tokens.pid744,
            "content": "record",
            "action": "export",
            "format": "csv",
            "type": "eav",
            "csvDelimiter": "",
            "fields": "curious_account_created_complete",
            "filter"
            "Logic": RedcapValues.PID744.curious_account_created_complete.filter_logic(
                "Complete"
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


def main() -> None:
    """Monitor Curious account invitations and send updates to REDCap."""
    auth = curious_authenticate()
    invitation_df = pull_data_from_curious(auth.access)
    if invitation_df.is_empty():
        logger.info("No invitations to update.")
        return
    invitation_df = check_activity_responses(
        auth.access,
        invitation_df,
        curious_variables.applet_ids["Healthy Brain Network Questionnaires"],
        curious_variables.activity_ids["Curious Account Created"],
    ).unique(subset=["record_id"], keep="last")
    n_records = push_to_redcap(invitation_df.write_csv())
    logger.info(
        "%d records updated in REDCap from Curious account creation.", n_records
    )
    if n_records != invitation_df.shape[0]:
        msg = (
            f"Expected {invitation_df.shape[0]} records to update but {n_records} did."
        )
        raise ValueError(msg)
    return


if __name__ == "__main__":
    main()
