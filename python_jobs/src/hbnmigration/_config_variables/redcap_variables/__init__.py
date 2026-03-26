"""REDCap variables. Put secrets in `./redcap_variables.py`."""

from ...utility_functions import ImportWithFallback

Endpoints = ImportWithFallback.module(
    ".redcap_variables", "Endpoints", "...utility_functions"
)
"""Ripple endpoints."""

headers = ImportWithFallback.literal(".redcap_variables", "headers", {})
"""REDCap headers."""

Tokens = ImportWithFallback.literal(".redcap_variables", "Tokens", NotImplemented)
"""REDCap API tokens."""

redcap_import_file = ImportWithFallback.literal(
    ".redcap_variables", "redcap_import_file", NotImplemented
)
"""Path to REDCap import data."""

redcap_update_file = ImportWithFallback.literal(
    ".redcap_variables", "redcap_update_file", NotImplemented
)
"""Path to REDCap update data."""
