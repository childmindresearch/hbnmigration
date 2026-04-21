"""Logging exceptions."""

from typing import Optional

from ..utility_functions.logging import setup_tsv_logger


class TSVLoggedError(Exception):
    """Exception that logs to TSV."""

    logger = setup_tsv_logger("mrn_error_log", "mrn_error_log.tsv")

    def __init__(self, mrn: int | str, error_message: Optional[str], attempt: str):
        """Initialize TSV logging exception."""
        self.mrn = mrn
        self.error_message = error_message or ""
        self.attempt = attempt

        # Log using logging library
        self.logger.exception(error_message, extra={"mrn": mrn, "attempt": attempt})

        super().__init__(f"MRN {mrn}: {error_message} (Attempt {attempt})")
