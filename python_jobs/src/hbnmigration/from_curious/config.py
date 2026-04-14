"""Nonsesitive for Curious API calls."""

from typing import Literal

from .._config_variables import curious_variables
from ..utility_functions import FieldDescriptor, ValueClass

AccountType = Literal["full", "limited"]

account_types: list[AccountType] = ["full", "limited"]

invitation_statuses: dict[Literal["not_invited", "pending", "invited"], int] = {
    "not_invited": 1,
    "pending": 2,
    "invited": 3,
}


def curious_authenticate(applet_name: str) -> curious_variables.Tokens:
    """Authenticate to Curious."""
    endpoints = curious_variables.Endpoints()
    tokens = curious_variables.Tokens(
        endpoints, curious_variables.AppletCredentials()[applet_name]
    )
    if not tokens:
        msg = f"Could not authenticate to {endpoints.host}"
        raise ConnectionError(msg)
    return tokens


class Values:
    """Values for Curious fields."""

    class HealthyBrainNetworkQuestionnaires(ValueClass):
        """Values for Healthy Brain Network Questionnaires applet."""

        class CuriousAccountCreated:
            """Values for Curious Account Created activity."""

            acount_created = FieldDescriptor(
                {"I confirm that I have created a Curious account": "1"}
            )
