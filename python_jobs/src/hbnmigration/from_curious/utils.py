"""Shared utilities for data processing from Curious to REDCap."""

import logging
from typing import cast, Optional, overload

import pandas as pd
import requests

from .._config_variables import curious_variables, redcap_variables
from ..from_redcap.from_redcap import response_index_reverse_lookup
from ..utility_functions import (
    CuriousActivity,
    CuriousAlert,
    CuriousAlertHttps,
    CuriousId,
    CuriousItem,
    fetch_api_data,
    T,
)

logger = logging.getLogger(__name__)

REDCAP_TOKEN = redcap_variables.Tokens.pid625

# Standard REDCap metadata fetch parameters
METADATA_PARAMS = {
    "content": "metadata",
    "action": "export",
    "format": "csv",
    "type": "eav",
    "csvDelimiter": "",
    "rawOrLabel": "raw",
    "rawOrLabelHeaders": "raw",
    "exportCheckboxLabel": "false",
    "exportSurveyFields": "false",
    "exportDataAccessGroups": "false",
    "returnFormat": "csv",
}

ALERTS_INSTRUMENT_FORM = "ra_alerts_child,ra_alerts_parent"
DEFAULT_EVENT_FOR_ALERTS = "admin_arm_1"


@overload
def call_curious_api(
    endpoint: str,
    tokens: curious_variables.Tokens,
    return_type: None = None,
    headers: Optional[dict[str, str]] = None,
) -> list | dict: ...
@overload
def call_curious_api(
    endpoint: str,
    tokens: curious_variables.Tokens,
    return_type: type[T],
    headers: Optional[dict[str, str]] = None,
) -> T: ...
def call_curious_api(
    endpoint: str,
    tokens: curious_variables.Tokens,
    return_type: Optional[type[T]] = None,
    headers: Optional[dict[str, str]] = None,
) -> T | list | dict:
    """Call Curious API."""
    response = requests.get(
        endpoint,
        headers=headers if headers else curious_variables.headers(tokens.access),
    )
    if response.status_code != requests.codes["okay"]:
        response.raise_for_status()
    result = response.json()["result"]
    if return_type:
        return cast(T, result)
    return result


def fetch_alerts_metadata(base_url: str) -> pd.DataFrame:
    """Fetch alerts instrument metadata from REDCap."""
    return fetch_api_data(
        base_url,
        redcap_variables.headers,
        {
            "token": REDCAP_TOKEN,
            "forms": ALERTS_INSTRUMENT_FORM,
            **METADATA_PARAMS,
        },
    )


def create_choice_lookup(
    alerts_instrument: pd.DataFrame,
) -> dict[tuple[str, str], str]:
    """Create lookup dictionary for mapping response values to REDCap indices."""
    choice_lookup_tuples = [
        lookup_tuple
        for lookup_tuple in [
            item
            for _, row in alerts_instrument.iterrows()
            for item in response_index_reverse_lookup(row)
        ]
        if lookup_tuple
    ]
    return {
        lookup_tuple[0:2]: str(lookup_tuple[2]) for lookup_tuple in choice_lookup_tuples
    }


def possible_alert_instruments(base_url: str) -> list[str]:
    """Return list of instruments that could trigger alerts."""
    return [
        col[:-7]
        for col in fetch_alerts_metadata(base_url).field_name.unique()
        if col.endswith("_alerts")
    ]


def get_instrument_event_mapping(base_url: str) -> dict[str, str]:
    """
    Get mapping of instruments to their designated events.

    Returns
    -------
    dict[str, str]
        Mapping of instrument_name to redcap_event_name

    """
    try:
        # Fetch instrument-event mapping from REDCap
        mapping = fetch_api_data(
            base_url,
            redcap_variables.headers,
            {
                "token": REDCAP_TOKEN,
                "content": "formEventMapping",
                "format": "json",
            },
        )

        if mapping.empty:
            logger.warning("No instrument-event mapping found in REDCap")
            return {}

        # Create dict mapping instrument name to event
        # If an instrument is used in multiple events, use the first one
        result = {}
        for _, row in mapping.iterrows():
            instrument = row.get("form")
            event = row.get("unique_event_name")
            if instrument and event and instrument not in result:
                result[instrument] = event

        return result

    except Exception as e:
        logger.warning("Could not fetch instrument-event mapping: %s", e)
        return {}


def get_alert_form_for_instrument(instrument_name: str) -> str:
    """
    Determine which alert form contains alerts for the given instrument.

    Parameters
    ----------
    instrument_name
        Name of the instrument (e.g., 'ace_p', 'cbcl')

    Returns
    -------
    str
        The alert form name ('ra_alerts_child' or 'ra_alerts_parent')

    """
    # Instruments that end with _p typically go to parent alerts
    # Instruments that end with _c or no suffix typically go to child alerts
    if instrument_name.endswith("_p"):
        return "ra_alerts_parent"
    if instrument_name.endswith("_c"):
        return "ra_alerts_child"
    # Default logic: check common patterns
    # This may need to be expanded based on your specific instruments
    parent_instruments = {"ace_p", "parent_baseline", "parent_followup"}
    if instrument_name in parent_instruments:
        return "ra_alerts_parent"
    return "ra_alerts_child"


def get_alert_field_event(base_url: str, instrument_name: str) -> str | None:
    """
    Get the event name for an alert field.

    Alert fields are stored on ra_alerts_parent or ra_alerts_child forms,
    which are typically on an admin event, NOT the same event as the data.

    Parameters
    ----------
    base_url
        REDCap API base URL
    instrument_name
        Name of the instrument (e.g., 'ace_p', 'cbcl')

    Returns
    -------
    str | None
        The event name for this instrument's alert field, or None if not found

    """
    # Determine which alert form this instrument uses
    alert_form = get_alert_form_for_instrument(instrument_name)

    # Get the instrument-event mapping
    instrument_events = get_instrument_event_mapping(base_url)

    # Look up the event for the ALERT FORM (not the data instrument)
    event = instrument_events.get(alert_form)

    if event:
        logger.info(
            "Alert field '%s_alerts' should use event '%s' (from form '%s')",
            instrument_name,
            event,
            alert_form,
        )
        return event

    logger.warning(
        "Could not determine event for alert form '%s' (instrument: '%s'), "
        "using default %s",
        alert_form,
        instrument_name,
        DEFAULT_EVENT_FOR_ALERTS,
    )
    return DEFAULT_EVENT_FOR_ALERTS


def get_field_to_event_mapping(base_url: str, field_names: list[str]) -> dict[str, str]:
    """
    Get mapping of field names to their REDCap event names.

    This fetches existing REDCap data and determines which event each field belongs to.

    Parameters
    ----------
    base_url
        REDCap API base URL
    field_names
        List of field names to look up

    Returns
    -------
    dict[str, str]
        Mapping of field_name to redcap_event_name

    """
    if not field_names:
        return {}

    # Fetch existing data for these fields
    params = {
        "token": REDCAP_TOKEN,
        "content": "record",
        "format": "json",
        "type": "flat",
        "fields": ",".join(field_names),
    }

    try:
        redcap_fields = fetch_api_data(
            base_url,
            redcap_variables.headers,
            params,
        )

        if redcap_fields.empty:
            logger.warning("No existing REDCap data found for fields: %s", field_names)
            return {}

        # Create mapping: field_name -> first event name found
        return cast(
            dict[str, str],
            redcap_fields.groupby("field_name")["redcap_event_name"].first().to_dict(),
        )

    except Exception as e:
        logger.warning("Could not fetch field-to-event mapping: %s", e)
        return {}


def get_activity(
    tokens: curious_variables.Tokens, activity_id: CuriousId
) -> CuriousActivity:
    """Get an activity from Curious."""
    return call_curious_api(
        tokens.endpoints.activity(activity_id), tokens, CuriousActivity
    )


def get_item(
    tokens: curious_variables.Tokens, activity_id: CuriousId, item_id: CuriousId
) -> CuriousItem:
    """Get an item from Curious."""
    activity = get_activity(tokens, activity_id)
    try:
        return next(item for item in activity["items"] if item["id"] == item_id)
    except StopIteration as stop_iteration:
        msg = f"Item {item_id} not found in {activity['name']} ({activity_id})."
        raise LookupError(msg) from stop_iteration


def alert_websocket_to_https(alert: CuriousAlert) -> CuriousAlertHttps:
    """Convert a `CuriousAlertWebsocket` to a `CuriousAlertHttps`."""
    return cast(
        CuriousAlertHttps,
        {
            "secretId" if key == "secret_id" else key: value
            for key, value in alert.items()
        },
    )
