"""Curious variables. Put secrets in `./curious_variables.py`."""

from ...utility_functions import CuriousId, ImportWithFallback

AppletCredentials = ImportWithFallback.module(
    ".curious_variables",
    "AppletCredentials",
    "...utility_functions.datatypes",
    "Credentials",
)
"""Applet credentials for decryption."""

Credentials = ImportWithFallback.module(
    ".curious_variables", "Credentials", "...utility_functions.datatypes"
)
"""Curious credentials."""

Endpoints = ImportWithFallback.module(
    ".curious_variables", "Endpoints", "...utility_functions.datatypes"
)
"""Curious endpoints."""

try:
    from .curious_variables import headers
except (ImportError, ModuleNotFoundError):

    def headers(token: str) -> dict[str, str]:
        """Curious headers."""
        return {}


owner_ids: dict[str, CuriousId] = ImportWithFallback.literal(
    ".curious_variables", "owner_ids", {}
)
"""Curious project owner IDs."""

applet_ids: dict[str, CuriousId] = ImportWithFallback.literal(
    ".curious_variables", "applet_ids", {}
)
"""Curious applet IDs."""

activity_ids: dict[str, CuriousId] = ImportWithFallback.literal(
    ".curious_variables", "activity_ids", {}
)
"""Curious activity IDs."""

Tokens = ImportWithFallback.module(
    ".curious_variables", "Tokens", "...utility_functions", "Tokens"
)
"""Curious tokens."""

__all__ = [
    "AppletCredentials",
    "Credentials",
    "Endpoints",
    "Tokens",
    "activity_ids",
    "applet_ids",
    "headers",
    "owner_ids",
]
