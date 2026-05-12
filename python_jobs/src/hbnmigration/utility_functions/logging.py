"""Set up logging."""

# TODO: Set up Iceberg
import atexit
import csv
from datetime import datetime, timedelta
import logging
from logging.handlers import RotatingFileHandler
import os
from pathlib import Path
import platform
import re
import sys
from typing import Optional, Set
import unicodedata

_ANSI_RE = re.compile(
    r"\x1b\[[0-9;]*+[a-zA-Z]"  # CSI sequences:  ESC [ <params> <letter>
    r"|\x1b\](?>[^\x07]*)\x07"  # OSC sequences:  ESC ] <payload> BEL
)
"""Possessive quantifier (*+) and atomic group ((?>...)) prevent catastrophic
backtracking (ReDoS)."""

_UNSAFE_CHARS = frozenset(("\u2028", "\u2029"))
"""Line separator ↵ and paragraph separator ¶."""
_UNSAFE_CATEGORIES = frozenset(("Cc", "Cf"))
"""Control characters and format characters."""

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


def invalid_fields_log_dir() -> Path:
    """Get directory for invalid fields logs."""
    return log_root_path() / "invalid_fields"


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


def safe_record_for_log(record: object) -> str:
    r"""
    Sanitize a value for safe inclusion in log output.

    Mitigates CRLF log injection attacks by removing characters that could
    be used to forge log entries, evade log analysis, or exploit log viewers.

    Strips the following:
        - CR/LF (\r, \n) — prevents basic log line injection
        - Other ASCII control characters (\x00-\x1f, \x7f) — removes tabs,
          backspaces, escape sequences, null bytes, etc.
        - Unicode line separators (U+2028, U+2029) — prevents injection via
          lesser-known Unicode newline characters
        - ANSI escape sequences (ESC[...m, etc.) — prevents terminal escape
          attacks in log viewers that interpret color/cursor codes
        - Unicode control category characters (Cc, Cf) — catches remaining
          control and formatting characters across all of Unicode

    Parameters
    ----------
    record
        The untrusted value to sanitize before logging.

    Returns
    -------
    str
        A sanitized copy of the value with dangerous characters removed.

    """
    text = str(record)
    text = text.replace("\r", "").replace("\n", "")
    result = _ANSI_RE.sub("", text)
    return "".join(
        ch
        for ch in result
        if ch not in _UNSAFE_CHARS
        and unicodedata.category(ch) not in _UNSAFE_CATEGORIES
    )


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


class InvalidFieldsLogger:
    """Manages daily logging of invalid fields with automatic rotation."""

    def __init__(self) -> None:
        """Initialize the invalid fields logger."""
        self._cache: Set[str] = set()
        self._log_path: Optional[Path] = None
        self._current_date: Optional[str] = None

    def _get_log_path(self) -> Path:
        """Get the path for today's invalid fields log."""
        today = datetime.now().strftime("%Y-%m-%d")

        if self._log_path is None or self._current_date != today:
            log_dir = invalid_fields_log_dir()
            log_dir.mkdir(parents=True, exist_ok=True)
            self._log_path = log_dir / f"{today}.txt"
            self._current_date = today

            # Clean up logs older than 7 days
            self._cleanup_old_logs()

            # Load existing fields from today's log
            self._cache = self._load_todays_fields()

        return self._log_path

    def _load_todays_fields(self) -> Set[str]:
        """Load already-logged invalid fields from today's log."""
        if self._log_path and self._log_path.exists():
            with open(self._log_path, "r", encoding="utf-8") as f:
                return {line.strip() for line in f if line.strip()}
        return set()

    def _cleanup_old_logs(self) -> None:
        """Remove invalid field logs older than 7 days."""
        log_dir = invalid_fields_log_dir()
        if not log_dir.exists():
            return

        cutoff_date = datetime.now() - timedelta(days=7)

        for log_file in log_dir.glob("*.txt"):
            try:
                # Parse date from filename (YYYY-MM-DD.txt)
                file_date = datetime.strptime(log_file.stem, "%Y-%m-%d")
                if file_date < cutoff_date:
                    log_file.unlink()
                    logger = logging.getLogger(__name__)
                    logger.info("Deleted old invalid fields log: %s", log_file)
            except (ValueError, OSError) as e:
                logger = logging.getLogger(__name__)
                logger.warning("Could not process log file %s: %s", log_file, e)

    def log_fields(self, invalid_fields: list[str]) -> list[str]:
        """
        Log invalid fields to today's log file, avoiding duplicates.

        Parameters
        ----------
        invalid_fields
            List of field names that were marked as invalid by the API.

        Returns
        -------
        new_fields
            List of newly-added invalid field names.

        """
        if not invalid_fields:
            return []

        log_path = self._get_log_path()
        new_fields = [field for field in invalid_fields if field not in self._cache]

        if new_fields:
            with open(log_path, "a", encoding="utf-8") as f:
                for field in new_fields:
                    f.write(f"{field}\n")
                    self._cache.add(field)

            logger = logging.getLogger(__name__)
            logger.info(
                "Logged %d new invalid field(s) to %s", len(new_fields), log_path
            )
        return new_fields

    def clear_cache(self) -> None:
        """Clear the cache (useful for testing or explicit cleanup)."""
        self._cache.clear()


# Singleton instance
_invalid_fields_logger = InvalidFieldsLogger()


def log_invalid_fields(invalid_fields: list[str]) -> list[str]:
    """
    Log invalid fields to today's log file, avoiding duplicates.

    Parameters
    ----------
    invalid_fields
        List of field names that were marked as invalid by the API.

    Returns
    -------
    new_fields
        List of newly-added invalid field names.

    """
    return _invalid_fields_logger.log_fields(invalid_fields)


# Register cleanup on exit
atexit.register(_invalid_fields_logger.clear_cache)


def redact_secret(secret: str, num_char: int = 4) -> str:
    """
    Return a masked representation safe for logs.

    Parameters
    ----------
    secret
        string to redact
    num_char
        number of characters to leave unredacted

    """
    return (
        f"{'█' * (len(secret) - num_char)}{secret[-num_char:]}"
        if len(secret) > num_char
        else "█" * len(secret)
    )
