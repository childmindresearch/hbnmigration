"""Curious variables. Put secrets in `./curious_variables.py`."""

from ...utility_functions import (
    Credentials as CredentialsABC,
    CuriousApplets,
    CuriousId,
    Endpoints as EndpointsType,
    ImportWithFallback,
    Tokens as TokensType,
)

AppletCredentials: type[CredentialsABC] = ImportWithFallback.module(
    ".curious_variables",
    "AppletCredentials",
    "...utility_functions.datatypes",
    "Credentials",
)
"""Applet credentials for decryption."""

applets: CuriousApplets = ImportWithFallback.literal(
    ".curious_variables", "applets", CuriousApplets()
)

Endpoints: type[EndpointsType] = ImportWithFallback.module(
    ".curious_variables", "Endpoints", "...utility_functions.datatypes"
)
"""Curious endpoints."""

try:
    from .curious_variables import headers
except ImportError, ModuleNotFoundError:

    def headers(token: str) -> dict[str, str]:
        """Curious headers."""
        return {}


owner_ids: dict[str, CuriousId] = ImportWithFallback.literal(
    ".curious_variables", "owner_ids", {}
)
"""Curious project owner IDs."""


Tokens: type[TokensType] = ImportWithFallback.module(
    ".curious_variables", "Tokens", "...utility_functions", "Tokens"
)
"""Curious tokens."""

__all__ = [
    "AppletCredentials",
    "Endpoints",
    "Tokens",
    "headers",
    "owner_ids",
]
