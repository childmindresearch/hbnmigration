"""Nonsesitive for Curious API calls."""

from typing import Literal

from .._config_variables import curious_variables

AccountType = Literal["full", "limited"]

account_types: list[AccountType] = ["full", "limited"]

invitation_statuses: dict[Literal["not_invited", "pending", "invited"], int] = {
    "not_invited": 1,
    "pending": 2,
    "invited": 3,
}


def curious_authenticate() -> curious_variables.Tokens:
    """Authenticate to Curious."""
    endpoints = curious_variables.Endpoints()
    tokens = curious_variables.Tokens(
        endpoints, curious_variables.Credentials.hbn_mindlogger
    )
    if not tokens:
        msg = f"Could not authenticate to {endpoints.host}"
        raise ConnectionError(msg)
    return tokens
