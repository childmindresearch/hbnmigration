"""
Transfer data from REDCap to REDCap.

For each subject in PID 247, if `intake_ready` == 1:
- push subject to PID 744, &
- set `indake_ready` = 2 in PID 247.
"""

import pandas as pd

from .._config_variables import redcap_variables
from ..exceptions import NoData
from ..utility_functions import initialize_logging, redcap_api_push
from .config import Fields, Values
from .from_redcap import fetch_data

Endpoints = redcap_variables.Endpoints()

logger = initialize_logging(__name__)


def update_source(df: pd.DataFrame) -> int:
    """
    Update `intake_ready` column in source project.

    Parameters
    ----------
    df
        destination DataFrame

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
    return redcap_api_push(
        df=df_274,
        token=redcap_variables.Tokens.pid247,
        url=Endpoints.base_url,
        headers=redcap_variables.headers,
    )


def main() -> None:
    """Transfer data from REDCap to REDCap."""
    try:
        # get data from PID247
        data247 = fetch_data(
            redcap_variables.Tokens.pid247,
            str(Fields.export_247.for_redcap744),
            Values.PID247.intake_ready.filter_logic("Ready to Send to Intake Redcap"),
        )
        data247["field_name"] = data247["field_name"].replace(
            Fields.rename.redcap247_to_redcap744
        )
        if data247.empty:
            raise NoData
        # rename columns for PID744
        data247["field_name"] = data247["field_name"].replace(
            Fields.rename.redcap247_to_redcap744
        )
        # format DataFrame for PID744
        df_744 = data247.loc[
            data247["field_name"].str.startswith(tuple(Fields.import_744))
        ]
        assert isinstance(df_744, pd.DataFrame)
        df_744 = (
            df_744.sort_values("redcap_repeat_instance", ascending=False)
            .drop_duplicates(subset=["record", "field_name"], keep="first")
            .drop(columns=["redcap_repeat_instrument", "redcap_repeat_instance"])
            .reset_index(drop=True)
        )
        decrement_mask = df_744["field_name"] == "permission_collab"
        # Convert to numeric and decrement
        decremented = (
            pd.to_numeric(df_744.loc[decrement_mask, "value"], errors="coerce") - 1
        )
        assert isinstance(decremented, pd.Series)
        # Convert back to string
        df_744.loc[decrement_mask, "value"] = decremented.astype(str)
        rows_imported_744 = redcap_api_push(
            df=df_744,
            token=redcap_variables.Tokens.pid744,
            url=Endpoints.base_url,
            headers=redcap_variables.headers,
        )
        if not rows_imported_744:
            raise NoData
        rows_updated_274 = update_source(df_744)
        assert rows_imported_744 == rows_updated_274, (
            f"rows imported to PID 744 ({rows_imported_744}) "
            f"≠ rows updated in PID 274 ({rows_updated_274})."
        )
    except NoData:
        logger.info("No data to transfer from PID 274 to PID 744.")


if __name__ == "__main__":
    main()
