"""Set up logging."""
# TODO: Set up Iceberg

import csv
from datetime import datetime
import logging
from logging.handlers import RotatingFileHandler
import os
from pathlib import Path
import platform
import sys
from typing import Optional

_initialized = False
"""Is logging initialized?"""


def log_root_path() -> Path:
    """Get root path for logging."""
    return Path(
        os.environ.get(
            "HBNMIGRATION_LOG_PATH",
            (
                Path("/home" if platform.system() == "Linux" else "/Users")
                / os.environ.get(
                    "USER_GROUP",
                    "/".join([os.environ.get("USER", ""), "hbnmigration"]).lstrip("/"),
                )
                / ".hbnmigration_logs"
            ),
        )
    )


class MaxLevelFilter(logging.Filter):
    """Exclude log records above a certain level."""

    def __init__(self, max_level: int) -> None:
        """
        Initialize filter.

        Parameters
        ----------
        max_level
            Maximum level to allow (inclusive)

        """
        super().__init__()
        self.max_level = max_level

    def filter(self, record: logging.LogRecord) -> bool:
        """Return True if record should be logged."""
        return record.levelno <= self.max_level


def initialize_logging(
    name: Optional[str] = None, max_gb: int = 10, backup_count: int = 3
) -> logging.Logger:
    """
    Initialize logging with rotation and separate files.

    Parameters
    ----------
    name
        `name` to provide to `logging.getLogger`

    max_gb
        Maximum size per log file (in GB) before rotation

    backup_count
        Number of backup files to keep

    Returns
    -------
    Logger

    """
    global _initialized  # noqa: PLW0603

    if _initialized:
        return logging.getLogger(name)

    max_bytes = int(max_gb * 1e9)

    log_dir = log_root_path()
    if not log_dir.exists():
        log_dir.mkdir(mode=0o770, parents=True, exist_ok=True)
    info_log = log_dir / "info.log"
    error_log = log_dir / "errors.log"

    # Detailed formatter with context
    detailed_formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - "
        "[%(filename)s:%(lineno)d] - %(funcName)s() - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Simple console formatter
    simple_formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(message)s", datefmt="%H:%M:%S"
    )

    # Handler 1: Info logs (DEBUG, INFO, WARNING) with rotation
    info_handler = RotatingFileHandler(
        info_log, maxBytes=max_bytes, backupCount=backup_count, encoding="utf-8"
    )
    info_handler.setLevel(logging.DEBUG)
    info_handler.setFormatter(detailed_formatter)
    # Exclude ERROR and CRITICAL
    info_handler.addFilter(MaxLevelFilter(logging.WARNING))

    # Handler 2: Error logs (ERROR, CRITICAL) with rotation
    error_handler = RotatingFileHandler(
        error_log, maxBytes=max_bytes, backupCount=backup_count, encoding="utf-8"
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(detailed_formatter)

    # Handler 3: Console (simple format, INFO+)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(simple_formatter)

    # Configure root logger
    logging.basicConfig(
        level=logging.INFO,
        handlers=[info_handler, error_handler, console_handler],
        force=True,
    )

    _initialized = True

    return logging.getLogger(name)


def setup_tsv_logger(name="tsv_logger", filename="error_log.tsv", level=logging.ERROR):
    """Configure a logger with TSV output."""
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Add TSV handler
    tsv_handler = TSVHandler(str(log_root_path() / filename))
    logger.addHandler(tsv_handler)

    # Optionally add console handler too
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(
        logging.Formatter("%(levelname)s - MRN %(mrn)s: %(message)s")
    )
    logger.addHandler(console_handler)

    return logger


class TSVHandler(logging.Handler):
    """Logging handler that writes to TSV format."""

    def __init__(self, filename="error_log.tsv") -> None:
        """Initialize TSV logging handler."""
        super().__init__()
        self.filename = Path(filename)
        self._ensure_headers()

    def _ensure_headers(self):
        """Create file with headers if it doesn't exist."""
        if not self.filename.exists():
            with open(self.filename, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f, delimiter="\t")
                writer.writerow(["datetime", "mrn", "error", "attempt"])

    def emit(self, record):
        """Write log record to TSV file."""
        try:
            # Extract custom fields from the log record
            mrn = getattr(record, "mrn", "N/A")
            attempt = getattr(record, "attempt", 1)

            with open(self.filename, "a", newline="", encoding="utf-8") as f:
                writer = csv.writer(f, delimiter="\t")
                writer.writerow(
                    [
                        datetime.fromtimestamp(record.created).isoformat(),
                        mrn,
                        record.getMessage(),
                        attempt,
                    ]
                )
        except Exception:
            self.handleError(record)
