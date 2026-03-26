"""Curious variables. Put secrets in `./curious_variables.py`."""

from ...utility_functions import ImportWithFallback

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

headers: dict[str, str] = ImportWithFallback.literal(
    ".curious_variables", "headers", {}
)
"""Curious headers."""

owner_ids: dict[str, str] = ImportWithFallback.literal(
    ".curious_variables", "owner_ids", {}
)
"""Curious project owner IDs."""

applet_ids: dict[str, str] = ImportWithFallback.literal(
    ".curious_variables", "applet_ids", {}
)
"""Curious applet IDs."""

Tokens = ImportWithFallback.module(
    ".curious_variables", "Tokens", "...utility_functions", "Tokens"
)
"""Curious tokens."""

__all__ = ["Credentials", "Endpoints", "headers"]
