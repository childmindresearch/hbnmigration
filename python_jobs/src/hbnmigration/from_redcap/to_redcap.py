"""Transfer data from REDCap to REDCap."""

import pandas as pd

from .._config_variables import redcap_variables
from ..exceptions import NoData
from ..utility_functions import fetch_api_data, initialize_logging
from .config import Fields as _Fields

Fields = _Fields()
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
        "filterLogic": "[intake_ready] = 1",
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


def main() -> None:
    """Transfer data from REDCap to REDCap."""
    data247 = fetch_data(redcap_variables.Tokens.pid247, str(Fields.export_247))  # noqa: F841


if __name__ == "__main__":
    main()
