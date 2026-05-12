"""Check for completed instruments from Curious activities; mark appropriate items."""

import logging
from typing import Optional

import pandas as pd

from .._config_variables import redcap_variables
from ..exceptions import NoData
from ..utility_functions import fetch_api_data, initialize_logging, redcap_api_push
from .config import Values
from .from_redcap import fetch_data

initialize_logging()
logger = logging.getLogger(__name__)


def get_tracked(metadata: pd.DataFrame, suffix: str) -> list[str]:
    """Return list of fields to check."""
    return [
        instrument
        for instrument in metadata["field_name"].unique()
        if instrument.endswith(suffix)
    ]


def get_redcap_curious_metadata(
    endpoint: str, token: str, forms: Optional[str] = None
) -> pd.DataFrame:
    """Get instrument metadata from REDCap API."""
    data = {
        "token": token,
        "content": "metadata",
        "format": "csv",
    }
    if forms:
        data["forms"] = forms
    return fetch_api_data(endpoint, redcap_variables.headers, data)


def get_curious_track_metadata(endpoint: str, token: str) -> pd.DataFrame:
    """Get "Curious Track" instrument metadata from REDCap API."""
    return get_redcap_curious_metadata(endpoint, token, "curious_track")


def subset_complete_data(data: pd.DataFrame, suffix) -> pd.DataFrame:
    """Get `*_complete` == 2 values."""
    data = data[data["field_name"].str.endswith(f"_{suffix}", na=False)].copy()
    data = data[
        pd.to_numeric(data["value"], errors="coerce")
        == int(Values.PID625.curious_complete["Complete"])
    ]
    data["value"] = Values.PID625.curious_track["yes"]
    data["redcap_event_name"] = "admin_arm_1"
    return data


def get_data(
    url: str, tokens: redcap_variables.Tokens, metadata: pd.DataFrame, new: bool
) -> pd.DataFrame:
    """Get data from REDCap."""
    columns = ["record", "field_name", "value", "redcap_event_name"]
    if new:
        token = tokens.pid891
        suffix = "complete"
    else:
        token = tokens.pid625
        suffix = "received"
    instruments = get_tracked(metadata, suffix)
    try:
        df = subset_complete_data(
            fetch_data(
                token,
                {
                    "fields": ",".join(
                        [
                            instrument
                            for instrument in instruments
                            if instrument.endswith(suffix)
                        ]
                    )
                },
            ),
            suffix,
        )
        return df[columns].copy()
    except NoData:
        return pd.DataFrame(columns=columns)


def filter_existing(
    new_data: pd.DataFrame, existing_data: pd.DataFrame
) -> pd.DataFrame:
    """Return only rows in `new_data` that don't already exist in `existing_data`."""
    df_unique = new_data.merge(existing_data, how="left", indicator=True)
    return df_unique[df_unique["_merge"] == "left_only"].drop(columns="_merge")


def rename_fields(new_df: pd.DataFrame, track_metadata: pd.DataFrame) -> pd.DataFrame:
    """Update fields to match the Curious Track field names."""
    # Extract instrument name from new_df field_name
    new_df["instrument"] = new_df["field_name"].str.replace(
        r"(_\d+)?_complete$", "", regex=True
    )

    # Find matches and non-matches
    mask_matched = new_df["instrument"].isin(
        [instrument[:-9] for instrument in get_tracked(track_metadata, "_received")]
    )

    # Log warnings for non-matches
    unmatched = new_df[~mask_matched]

    for _, row in unmatched.iterrows():
        logger.warning("No match found for: %s", row["field_name"])

    # Drop non-matches
    new_df = new_df[mask_matched].copy()

    # Update field_name to match track format (instrument + "_received")
    new_df["field_name"] = new_df["instrument"] + "_received"

    # Clean up helper column
    return new_df.drop(columns="instrument")


def update_curious_track(token: str, url: str, new_data: pd.DataFrame) -> None:
    """Update "Curious Track" in REDCap."""
    redcap_api_push(new_data, token, url, redcap_variables.headers)


def main() -> None:
    """Update `curious_track`."""
    endpoints = redcap_variables.Endpoints()
    tokens = redcap_variables.Tokens()
    track_metadata = get_curious_track_metadata(endpoints.base_url, tokens.pid625)
    activity_metadata = get_redcap_curious_metadata(endpoints.base_url, tokens.pid891)
    new_data = get_data(endpoints.base_url, tokens, activity_metadata, True)
    existing_data = get_data(endpoints.base_url, tokens, track_metadata, False)
    new_data = rename_fields(filter_existing(new_data, existing_data), track_metadata)
    for project in [625, 891]:
        update_curious_track(
            getattr(tokens, f"pid{project}"), endpoints.base_url, new_data
        )
    logger.info(
        "Marked Curious instruments %s complete across %d participants in REDCap.",
        [
            instrument[:-9]
            for instrument in new_data["field_name"].unique()
            if instrument.endswith("_received")
        ],
        new_data["record"].nunique(),
    )


if __name__ == "__main__":
    main()
