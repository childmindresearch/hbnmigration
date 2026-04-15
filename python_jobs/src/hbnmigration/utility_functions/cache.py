"""Data deduplication cache for minute-by-minute transfers."""

from datetime import datetime, timedelta
import hashlib
import json
import logging
import os
from pathlib import Path
from typing import Any, Optional, Sequence

logger = logging.getLogger(__name__)

YESTERDAY_DATE = datetime.now() - timedelta(days=1)
"""Datetime representation of yesterday."""

YESTERDAY = YESTERDAY_DATE.isoformat()
"""String representation of yesterday."""


def get_cache_dir() -> Path:
    """Get cache directory, respecting HBNMIGRATION_CACHE_DIR env var for testing."""
    cache_dir_env = os.environ.get("HBNMIGRATION_CACHE_DIR")
    if cache_dir_env:
        return Path(cache_dir_env)
    return Path.home() / ".hbnmigration_cache"


def get_recent_time_window(
    minutes_back: int = 2,
    allow_full_day_fallback: bool = True,
) -> tuple[str, str]:
    """
    Get ISO timestamp window for API queries (minute-by-minute transfers).

    Automatically falls back to full-day pull (yesterday to now) if:
    - HBNMIGRATION_RECOVERY_MODE env var is set, OR
    - Cache is missing/stale (hasn't processed in > 2 hours)

    Parameters
    ----------
    minutes_back : int, optional
        How many minutes back to pull data from (default 2 for minute-by-minute).
    allow_full_day_fallback : bool, optional
        Allow automatic fallback to full-day pull on downtime. Default True.

    Returns
    -------
    tuple[str, str]
        (start_time, end_time) as ISO format strings for use in API queries.
        On recovery: returns (yesterday, now) for full-day pull.

    Examples
    --------
    >>> start, end = get_recent_time_window(2)
    >>> # Returns 2-minute window: ('2026-04-06T14:58:30', '2026-04-06T15:00:30')

    >>> # With stale cache or recovery mode:
    >>> # Returns full day: ('2026-04-05T00:00:00', '2026-04-06T15:00:30')

    """
    # Check for manual recovery override
    from ..config import Config  # noqa: PLC0415

    if Config.RECOVERY_MODE:
        logger.warning("Recovery mode enabled - pulling full day's data")
        now = datetime.now()
        return YESTERDAY, now.isoformat()

    # Check if cache is stale (no activity in > 2 hours = downtime detected)
    if allow_full_day_fallback:
        stale_threshold = datetime.now() - timedelta(hours=2)
        cache_dir = get_cache_dir()

        if cache_dir.exists():
            # Find most recent cache file
            cache_files = list(cache_dir.glob("*_cache.json"))
            if cache_files:
                most_recent = max(cache_files, key=lambda p: p.stat().st_mtime)
                last_modified = datetime.fromtimestamp(most_recent.stat().st_mtime)

                if last_modified < stale_threshold:
                    logger.warning(
                        "Cache stale (last update: %s). Downtime detected - "
                        "pulling full day's data for recovery",
                        last_modified.isoformat(),
                    )
                    now = datetime.now()
                    return YESTERDAY, now.isoformat()

    # Normal operation: use 2-minute window
    now = datetime.now()
    start = now - timedelta(minutes=minutes_back)
    return start.isoformat(), now.isoformat()


class DataCache:
    """
    Persistent cache for tracking processed records to reduce duplicate pushes.

    Features:
    - File-based JSON persistence
    - Configurable TTL for cache entries
    - Deduplication based on data hash
    - Per-job cache isolation
    """

    def __init__(
        self,
        job_name: str,
        cache_dir: Optional[Path] = None,
        ttl_minutes: int = 2,
    ) -> None:
        """
        Initialize data cache.

        Parameters
        ----------
        job_name
            Unique identifier for the job (e.g., 'data_to_redcap', 'to_curious')
        cache_dir
            Directory to store cache files. Defaults to ~/.hbnmigration_cache
        ttl_minutes
            Time-to-live for cache entries in minutes. Default 2
            (for minute-by-minute jobs).

        """
        self.job_name = job_name
        self.ttl_minutes = ttl_minutes

        if cache_dir is None:
            cache_dir = get_cache_dir()

        self.cache_dir = cache_dir
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        self.cache_file = self.cache_dir / f"{job_name}_cache.json"
        self._cache: dict[str, dict[str, Any]] = self._load_cache()

    def _load_cache(self) -> dict[str, dict[str, Any]]:
        """Load cache from disk."""
        if not self.cache_file.exists():
            return {}

        try:
            with open(self.cache_file, "r") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            logger.warning("Failed to load cache from %s: %s", self.cache_file, e)
            return {}

    def _save_cache(self) -> None:
        """Save cache to disk."""
        try:
            with open(self.cache_file, "w") as f:
                json.dump(self._cache, f, indent=2)
        except IOError as e:
            logger.warning("Failed to save cache to %s: %s", self.cache_file, e)

    def _compute_hash(self, data: Any) -> str:
        """Compute SHA256 hash of data for deduplication."""
        data_str = json.dumps(data, sort_keys=True, default=str)
        return hashlib.sha256(data_str.encode()).hexdigest()

    def _is_expired(self, timestamp: str) -> bool:
        """Check if cache entry has expired."""
        try:
            entry_time = datetime.fromisoformat(timestamp)
            expiry_time = entry_time + timedelta(minutes=self.ttl_minutes)
            return datetime.now() > expiry_time
        except ValueError, TypeError:
            return True

    def is_processed(self, record_id: str | int, data: Optional[Any] = None) -> bool:
        """
        Check if a record has already been processed.

        Parameters
        ----------
        record_id : str | int
            Unique identifier for the record
        data : Any, optional
            Data to check against cached hash. If provided, verifies data hasn't
            changed. If not provided, only checks if record exists.

        Returns
        -------
        bool
            True if record was processed and (data unchanged or not provided)

        """
        record_id_str = str(record_id)

        if record_id_str not in self._cache:
            return False

        cached_entry = self._cache[record_id_str]

        # Check if expired
        if self._is_expired(cached_entry["timestamp"]):
            del self._cache[record_id_str]
            self._save_cache()
            return False

        # If data provided, check hash
        if data is not None:
            current_hash = self._compute_hash(data)
            cached_hash = cached_entry.get("data_hash")
            if current_hash != cached_hash:
                return False

        return True

    def mark_processed(
        self,
        record_id: str | int,
        data: Optional[Any] = None,
        metadata: Optional[dict[str, Any]] = None,
    ) -> None:
        """
        Mark a record as processed.

        Parameters
        ----------
        record_id : str | int
            Unique identifier for the record
        data : Any, optional
            Actual data that was processed (for hash computation)
        metadata : dict, optional
            Additional metadata to store with cache entry

        """
        record_id_str = str(record_id)

        cache_entry = {
            "timestamp": datetime.now().isoformat(),
            "processed": True,
        }

        if data is not None:
            cache_entry["data_hash"] = self._compute_hash(data)

        if metadata:
            cache_entry["metadata"] = metadata

        self._cache[record_id_str] = cache_entry
        self._save_cache()

    def bulk_mark_processed(
        self,
        record_ids: Sequence[str | int],
        metadata: Optional[dict[str, Any]] = None,
    ) -> None:
        """
        Mark multiple records as processed in batch.

        Parameters
        ----------
        record_ids : list[str | int]
            List of record identifiers
        metadata : dict, optional
            Metadata to apply to all records

        """
        for record_id in record_ids:
            self.mark_processed(record_id, metadata=metadata)

    def get_unprocessed_records(
        self,
        record_ids: Sequence[str | int],
        data_dict: Optional[dict[str | int, Any]] = None,
    ) -> list[str | int]:
        """
        Filter list to only unprocessed records.

        Parameters
        ----------
        record_ids : list[str | int]
            List of record IDs to filter
        data_dict : dict, optional
            Mapping of record_id to data for hash verification

        Returns
        -------
        list[str | int]
            Records that haven't been processed yet

        """
        unprocessed = []
        for record_id in record_ids:
            data = data_dict.get(record_id) if data_dict else None
            if not self.is_processed(record_id, data):
                unprocessed.append(record_id)

        return unprocessed

    def clear_cache(self) -> None:
        """Clear all cached entries for this job."""
        self._cache.clear()
        self._save_cache()
        logger.info("Cache cleared for job: %s", self.job_name)

    def clear_expired(self) -> int:
        """
        Remove expired entries from cache.

        Returns
        -------
        int
            Number of entries removed

        """
        expired_keys = [
            key
            for key, entry in self._cache.items()
            if self._is_expired(entry["timestamp"])
        ]

        for key in expired_keys:
            del self._cache[key]

        if expired_keys:
            self._save_cache()
            logger.info(
                "Cleared %d expired cache entries for job: %s",
                len(expired_keys),
                self.job_name,
            )

        return len(expired_keys)

    def get_stats(self) -> dict[str, Any]:
        """
        Get cache statistics.

        Returns
        -------
        dict
            Cache statistics including size, entries, etc.

        """
        # Find most recent entry timestamp
        latest_timestamp = None
        if self._cache:
            timestamps = [
                entry.get("timestamp")
                for entry in self._cache.values()
                if entry.get("timestamp")
            ]
            if timestamps:
                try:
                    latest_timestamp = max(
                        str(ts) for ts in timestamps if ts is not None
                    )
                except TypeError, ValueError:
                    latest_timestamp = None

        return {
            "job_name": self.job_name,
            "cache_file": str(self.cache_file),
            "total_entries": len(self._cache),
            "ttl_minutes": self.ttl_minutes,
            "file_size_bytes": self.cache_file.stat().st_size
            if self.cache_file.exists()
            else 0,
            "last_activity": latest_timestamp,
        }


__all__ = ["DataCache"]
