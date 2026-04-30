"""Nonsesitive for Curious API calls."""

from typing import ClassVar, Literal

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


class Fields:
    """REDCap field names for Curious invitation data."""

    common: ClassVar[list[str]] = [
        "record_id",
        "redcap_event_name",
    ]
    """Fields that are always included regardless of account context."""

    responder: ClassVar[list[str]] = [
        "curious_account_created_source_secret_id",
        "curious_account_created_invite_status",
        "curious_account_created_account_created_response",
        "curious_account_created_responder_complete",
    ]
    """Fields specific to responder accounts."""

    child: ClassVar[list[str]] = [
        "curious_account_created_source_secret_id_c",
        "curious_account_created_invite_status_c",
        "curious_account_created_child_complete",
    ]
    """Fields specific to child accounts."""

    @classmethod
    def for_context(cls, ctx: Literal["responder", "child"]) -> list[str]:
        """Return all REDCap fields for the given account context."""
        return [*cls.common, *(cls.responder if ctx == "responder" else cls.child)]


class Values:
    """Values for Curious fields."""

    class HealthyBrainNetworkQuestionnaires(ValueClass):
        """Values for Healthy Brain Network Questionnaires applet."""

        class CuriousAccountCreated:
            """Values for Curious Account Created activity."""

            acount_created = FieldDescriptor(
                {"I confirm that I have created a Curious account": "1"}
            )
