"""Custom exceptions."""

from .logging import TSVLoggedError


class NoData(Exception):  # noqa: N818
    """No data to process."""


__all__ = ["NoData", "TSVLoggedError"]
