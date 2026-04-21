"""
Cache utilities for tracking processed items.

NOTE: This cache is used for tracking Curious alerts and other non-REDCap-push
operations. REDCap push operations (to_redcap.py and to_curious.py) do NOT use
caching and operate directly on trigger events.
"""

from datetime import datetime, timedelta, UTC
import json
import logging
import os
from pathlib import Path
import time
from typing import Any

logger = logging.getLogger(__name__)


class DataCache:
    """
    Cache for tracking processed data items.

    This cache is used for operations like Curious alert tracking where we need
    to avoid duplicate processing. It is NOT used for REDCap push operations which
    are triggered by webhooks.

    The cache stores processed item identifiers with timestamps and optional metadata.
    Items expire after a configurable TTL (time-to-live).
    """

    def __init__(
        self,
        cache_name: str,
        ttl_minutes: int = 60,
        cache_dir: str | None = None,
    ) -> None:
        """
        Initialize the cache.

        Args:
            cache_name: Name of this cache (used as filename).
            ttl_minutes: Time-to-live in minutes for cache entries.
            cache_dir: Directory to store cache files. Defaults to /tmp/hbn_cache.

        """
        self.cache_name = cache_name
        self.ttl_minutes = ttl_minutes

        if cache_dir is None:
            cache_dir = str(get_cache_dir())

        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        self.cache_file = self.cache_dir / f"{cache_name}.json"
        self._cache: dict[str, dict[str, Any]] = {}
        self._load_cache()
        self._cleanup_expired()

    def _load_cache(self) -> None:
        """Load cache from disk."""
        if self.cache_file.exists():
            try:
                with open(self.cache_file, "r") as f:
                    self._cache = json.load(f)
                logger.debug("Loaded cache from %s", self.cache_file)
            except (json.JSONDecodeError, IOError) as e:
                logger.warning("Could not load cache from %s: %s", self.cache_file, e)
                self._cache = {}
        else:
            self._cache = {}

    def _save_cache(self) -> None:
        """Save cache to disk."""
        try:
            with open(self.cache_file, "w") as f:
                json.dump(self._cache, f, indent=2)
            logger.debug("Saved cache to %s", self.cache_file)
        except IOError:
            logger.exception("Could not save cache to %s", self.cache_file)

    def _cleanup_expired(self) -> None:
        """Remove expired entries from cache."""
        now = time.time()
        ttl_seconds = self.ttl_minutes * 60

        expired_keys = [
            key
            for key, entry in self._cache.items()
            if now - entry.get("timestamp", 0) > ttl_seconds
        ]

        for key in expired_keys:
            del self._cache[key]

        if expired_keys:
            logger.debug("Removed %d expired entries from cache", len(expired_keys))
            self._save_cache()

    def is_processed(self, identifier: str) -> bool:
        """
        Check if an item has been processed.

        Args:
            identifier: Unique identifier for the item.

        Returns:
            True if item was recently processed (within TTL), False otherwise.

        """
        self._cleanup_expired()
        return identifier in self._cache

    def mark_processed(
        self,
        identifier: str,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """
        Mark an item as processed.

        Args:
            identifier: Unique identifier for the item.
            metadata: Optional metadata to store with the entry.

        """
        self._cache[identifier] = {
            "timestamp": time.time(),
            "processed_at": datetime.now().isoformat(),
            "metadata": metadata or {},
        }
        self._save_cache()

    def remove(self, identifier: str) -> None:
        """
        Remove an item from the cache.

        Args:
            identifier: Unique identifier for the item.

        """
        if identifier in self._cache:
            del self._cache[identifier]
            self._save_cache()
            logger.debug("Removed %s from cache", identifier)

    def clear(self) -> None:
        """Clear all entries from the cache."""
        self._cache = {}
        self._save_cache()
        logger.info("Cleared cache %s", self.cache_name)

    def get_unprocessed_records(
        self,
        record_ids: list[str | int],
    ) -> list[str | int]:
        """
        Filter list to only unprocessed records.

        Args:
            record_ids: List of record IDs to filter.

        Returns:
            Records that haven't been processed yet.

        """
        self._cleanup_expired()
        return [
            record_id
            for record_id in record_ids
            if not self.is_processed(str(record_id))
        ]

    def bulk_mark_processed(
        self,
        record_ids: list[str | int],
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """
        Mark multiple records as processed in batch.

        Args:
            record_ids: List of record identifiers.
            metadata: Metadata to apply to all records.

        """
        for record_id in record_ids:
            self.mark_processed(str(record_id), metadata=metadata)

    def get_stats(self) -> dict[str, Any]:
        """
        Get cache statistics.

        Returns:
            Dictionary with cache statistics.

        """
        self._cleanup_expired()

        stats: dict[str, Any] = {
            "cache_name": self.cache_name,
            "total_entries": len(self._cache),
            "ttl_minutes": self.ttl_minutes,
            "cache_file": str(self.cache_file),
            "file_size_bytes": self.cache_file.stat().st_size
            if self.cache_file.exists()
            else 0,
        }

        if self._cache:
            timestamps = [entry.get("timestamp", 0) for entry in self._cache.values()]
            stats["oldest_entry"] = datetime.fromtimestamp(min(timestamps)).isoformat()
            stats["newest_entry"] = datetime.fromtimestamp(max(timestamps)).isoformat()
            stats["last_activity"] = stats["newest_entry"]

        return stats

    def get_all_entries(self) -> dict[str, dict[str, Any]]:
        """
        Get all cache entries.

        Returns:
            Dictionary of all cache entries.

        """
        self._cleanup_expired()
        return self._cache.copy()


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
        now = datetime.now(UTC)
        return YESTERDAY.replace("+00:00", ""), now.isoformat().replace("+00:00", "")

    # Check if cache is stale (no activity in > 2 hours = downtime detected)
    if allow_full_day_fallback:
        stale_threshold = datetime.now(UTC) - timedelta(hours=2)
        cache_dir = get_cache_dir()

        if cache_dir.exists():
            # Find most recent cache file
            cache_files = list(cache_dir.glob("*_cache.json"))
            if cache_files:
                most_recent = max(cache_files, key=lambda p: p.stat().st_mtime)
                last_modified = datetime.fromtimestamp(
                    most_recent.stat().st_mtime, tz=UTC
                )

                if last_modified < stale_threshold:
                    logger.warning(
                        "Cache stale (last update: %s). Downtime detected - "
                        "pulling full day's data for recovery",
                        last_modified.isoformat(),
                    )
                    now = datetime.now(UTC)
                    return YESTERDAY.replace("+00:00", ""), now.isoformat().replace(
                        "+00:00", ""
                    )

    # Normal operation: use 2-minute window
    now = datetime.now(UTC)
    start = now - timedelta(minutes=minutes_back)
    return start.isoformat(timespec="seconds").replace("+00:00", ""), now.isoformat(
        timespec="seconds"
    ).replace("+00:00", "")
