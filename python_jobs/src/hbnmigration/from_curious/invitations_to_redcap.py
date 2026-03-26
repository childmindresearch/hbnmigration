"""Monitor Curious account invitations and send updates to REDCap."""

import logging
from typing import Literal

import pandas as pd
import requests

from mindlogger_data_export.outputs import RedcapImportFormat

from .._config_variables import curious_variables, redcap_variables
from ..from_redcap.config import Values as RedcapValues
from ..utility_functions import (
    CuriousId,
    fetch_api_data,
    initialize_logging,
    yesterday_or_more_recent,
)
from .config import curious_authenticate, invitation_statuses
from .decryption import decrypt_single, get_applet_encryption, pair_qanda

initialize_logging()
logger = logging.getLogger(__name__)
PROJECT_STATUS: Literal["dev", "prod"] = "dev"


class Endpoints:
    """Initialized endpoints."""

    Curious = curious_variables.Endpoints()
    """Curious endpoints."""
    Redcap = redcap_variables.Endpoints()
    """REDCap endpoints."""


def check_activity_response(
    token, respondent_id: CuriousId, applet_id: CuriousId, activity_id: CuriousId
) -> pd.Series:
    """Check for response to activity."""
    encryption = get_applet_encryption(Endpoints.Curious.applet(applet_id), token)
    response = requests.get(
        Endpoints.Curious.applet_activity_answers_list(applet_id, activity_id)
        + f"?respondentId={respondent_id}",
        headers=curious_variables.headers(token),
    )
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
                breakpoint()
                paired_responses = pair_qanda(decrypted_answer)  # noqa: F841
                # for response in paired_responses:
                #     if response['item'].get('queston', {}).get('en', None) == (
                #         "Please click below to confirm that you have created a "
                #         "Curious account"
                #     ):

                format_for_redcap(decrypted_answer)
    return pd.Series()


def check_activity_responses(
    token: str, df: pd.DataFrame, applet_id: CuriousId, activity_id: CuriousId
) -> pd.DataFrame:
    """Check for responses to activity."""
    for row in df.iterrows():
        check_activity_response(token, row[1]["respondent_id"], applet_id, activity_id)
    return df


def create_invitation_record(respondent: dict, applet_id: CuriousId) -> pd.Series:
    """Create a Series for a respondent with MRN and invitation status."""
    details: list[dict] = [
        detail for detail in respondent["details"] if detail["appletId"] == applet_id
    ]
    if not details:
        return pd.Series()
    detail = details[-1]
    secret_id: str = detail["respondentSecretId"]
    try:
        secret_id = str(int(secret_id))
    except ValueError:
        secret_id = str(secret_id)
    if not secret_id.endswith("_P"):
        return pd.Series()
    return pd.Series(
        {
            "record_id": secret_id[:-2],
            "curious_account_created_source_secret_id": secret_id,
            "curious_account_created_invite_status": invitation_statuses[
                respondent["status"]
            ],
            "redcap_event_name": "curious_parent_arm_1",
            "curious_account_created_complete": 0,
            "respondent_id": detail["subjectId"],
        }
    )


def format_for_redcap(ml_data):
    """Format response data for REDCap import."""
    formatter = RedcapImportFormat(
        project={"curious_account_created": "curious_parent_arm_1"}
    )
    outputs = formatter.produce(ml_data)  # noqa: F841
    breakpoint()


def pull_data_from_curious(token: str) -> pd.DataFrame:
    """Pull data from Curious."""
    response = requests.get(
        Endpoints.Curious.invitation_statuses(
            curious_variables.owner_ids["Healthy Brain Network (HBN)"],
            curious_variables.applet_ids["Healthy Brain Network Questionnaires"],
        ),
        headers=curious_variables.headers(token),
    )
    invitation_df = pd.DataFrame()
    if response.status_code == requests.codes["okay"]:
        invitation_df = update_already_completed(
            pd.concat(
                [
                    create_invitation_record(
                        respondent,
                        curious_variables.applet_ids[
                            "Healthy Brain Network Questionnaires"
                        ],
                    )
                    for respondent in response.json()["result"]
                    if (
                        respondent["lastSeen"] is None
                        or yesterday_or_more_recent(respondent["lastSeen"])
                    )
                ],
                axis=1,
            ).transpose()
        )
    else:
        response.raise_for_status()
    return invitation_df


def push_to_redcap(csv_data: str) -> None:
    """Push data to RedCap."""
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


def update_already_completed(df: pd.DataFrame) -> pd.DataFrame:
    """For any already-completed records in REDCap, don't unmark them complete."""
    # Series of record IDs for already-completed parent invitation activities.
    already_completed: pd.Series = fetch_api_data(
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
    ).get("record", pd.Series())
    df.loc[
        df["record_id"].isin(already_completed), "curious_account_created_complete"
    ] = None
    return df.dropna()


def main() -> None:
    """Monitor Curious account invitations and send updates to REDCap."""
    auth = curious_authenticate()
    invitation_df = pull_data_from_curious(auth.access)
    check_activity_responses(
        auth.access,
        invitation_df,
        curious_variables.applet_ids["Healthy Brain Network Questionnaires"],
        curious_variables.activity_ids["Curious Account Created"],
    )
    breakpoint()


if __name__ == "__main__":
    main()
