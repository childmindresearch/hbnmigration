"""Ripple variables. Put secrets in `./ripple_variables.py`."""

from ...utility_functions import ImportWithFallback

column_dict = ImportWithFallback.literal(".ripple_variables", "column_dict", {})
"""Return a dict for including list of columns in Ripple API calls."""

Endpoints = ImportWithFallback.module(
    ".ripple_variables", "Endpoints", "...utility_functions.datatypes"
)
"""Ripple endpoints."""

headers = ImportWithFallback.literal(".ripple_variables", "headers", {})
"""Ripple headeres."""

study_ids = ImportWithFallback.literal(".ripple_variables", "study_ids", {})
"""Ripple study IDs."""

ripple_import_file = ImportWithFallback.literal(
    ".ripple_variables", "ripple_import_file", NotImplemented
)
