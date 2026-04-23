"""Tests for cache recovery mode functionality."""

from datetime import datetime, timedelta
from pathlib import Path
import tempfile
import time
from typing import Any
from unittest.mock import patch

import pytest

from hbnmigration.utility_functions.cache import DataCache, get_recent_time_window


class TestGetRecentTimeWindowRecoveryMode:
    """Tests for get_recent_time_window recovery mode behavior."""

    def test_normal_operation_returns_2_minute_window(self):
        """Should return 2-minute window during normal operation."""
        with patch("hbnmigration.config.Config") as mock_config:
            mock_config.RECOVERY_MODE = False

            start, end = get_recent_time_window(
                minutes_back=2, allow_full_day_fallback=True
            )

            start_dt = datetime.fromisoformat(start)
            end_dt = datetime.fromisoformat(end)

            # Window should be approximately 2 minutes
            delta = end_dt - start_dt
            assert delta.total_seconds() <= 120  # At most 2 minutes
            assert delta.total_seconds() >= 119  # At least ~1:59

    def test_recovery_mode_env_var_returns_full_day(self):
        """Should return full-day window when Config.RECOVERY_MODE is True."""
        with patch("hbnmigration.config.Config") as mock_config:
            mock_config.RECOVERY_MODE = True

            start, end = get_recent_time_window(
                minutes_back=2, allow_full_day_fallback=True
            )

            start_dt = datetime.fromisoformat(start)
            end_dt = datetime.fromisoformat(end)

            # Window should be at least 1 day (YESTERDAY constant to now)
            delta = end_dt - start_dt
            assert delta.total_seconds() >= 86400, (
                f"Expected window at least 24 hours (86400s), "
                f"got {delta.total_seconds()}s"
            )

    def test_recovery_mode_logs_warning(self, caplog):
        """Should log warning when recovery mode is enabled."""
        with patch("hbnmigration.config.Config") as mock_config:
            mock_config.RECOVERY_MODE = True

            get_recent_time_window(minutes_back=2, allow_full_day_fallback=True)

            # Should have logged the recovery mode warning
            assert any(
                "Recovery mode enabled" in record.message for record in caplog.records
            )

    def test_stale_cache_triggers_recovery_fallback(self, caplog):
        """Should return full-day window when cache is stale (>2 hours)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir)
            cache_dir.mkdir(exist_ok=True)

            # Create a cache file that's 3 hours old
            old_time = datetime.now() - timedelta(hours=3)
            cache_file = cache_dir / "test_cache.json"
            cache_file.write_text("{}")

            # Set file modification time to 3 hours ago
            mtime = old_time.timestamp()
            import os

            os.utime(cache_file, (mtime, mtime))

            with patch("hbnmigration.config.Config") as mock_config:
                mock_config.RECOVERY_MODE = False
                with patch(
                    "hbnmigration.utility_functions.cache.get_cache_dir"
                ) as mock_get_cache_dir:
                    mock_get_cache_dir.return_value = cache_dir

                    start, end = get_recent_time_window(
                        minutes_back=2, allow_full_day_fallback=True
                    )

                    start_dt = datetime.fromisoformat(start)
                    end_dt = datetime.fromisoformat(end)

                    # Window should be approximately 1 day due to stale cache
                    delta = end_dt - start_dt
                    assert delta.total_seconds() >= 86400

    def test_stale_cache_logs_recovery_message(self, caplog):
        """Should log downtime recovery message when cache is stale."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir)
            cache_dir.mkdir(exist_ok=True)

            # Create a cache file that's 3 hours old
            old_time = datetime.now() - timedelta(hours=3)
            cache_file = cache_dir / "test_cache.json"
            cache_file.write_text("{}")

            # Set file modification time to 3 hours ago
            mtime = old_time.timestamp()
            import os

            os.utime(cache_file, (mtime, mtime))

            with patch("hbnmigration.config.Config") as mock_config:
                mock_config.RECOVERY_MODE = False
                with patch(
                    "hbnmigration.utility_functions.cache.get_cache_dir"
                ) as mock_get_cache_dir:
                    mock_get_cache_dir.return_value = cache_dir

                    get_recent_time_window(minutes_back=2, allow_full_day_fallback=True)

                    # Should have logged the downtime recovery message
                    assert any(
                        "Downtime detected" in record.message
                        for record in caplog.records
                    )

    def test_fresh_cache_does_not_trigger_recovery(self):
        """Should return 2-minute window when cache is fresh (<2 hours)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir)
            cache_dir.mkdir(exist_ok=True)

            # Create a cache file that's 1 hour old (fresh)
            fresh_time = datetime.now() - timedelta(hours=1)
            cache_file = cache_dir / "test_cache.json"
            cache_file.write_text("{}")

            # Set file modification time to 1 hour ago
            mtime = fresh_time.timestamp()
            import os

            os.utime(cache_file, (mtime, mtime))

            with patch("hbnmigration.config.Config") as mock_config:
                mock_config.RECOVERY_MODE = False
                with patch(
                    "hbnmigration.utility_functions.cache.get_cache_dir"
                ) as mock_get_cache_dir:
                    mock_get_cache_dir.return_value = cache_dir

                    start, end = get_recent_time_window(
                        minutes_back=2, allow_full_day_fallback=True
                    )

                    start_dt = datetime.fromisoformat(start)
                    end_dt = datetime.fromisoformat(end)

                    # Window should be approximately 2 minutes (normal operation)
                    delta = end_dt - start_dt
                    assert delta.total_seconds() <= 120

    def test_no_cache_dir_does_not_crash(self):
        """Should handle gracefully when cache directory doesn't exist."""
        with patch("hbnmigration.config.Config") as mock_config:
            mock_config.RECOVERY_MODE = False
            with patch(
                "hbnmigration.utility_functions.cache.get_cache_dir"
            ) as mock_get_cache_dir:
                # Return a non-existent directory
                mock_get_cache_dir.return_value = Path("/nonexistent/path")

                # Should not crash
                start, end = get_recent_time_window(
                    minutes_back=2, allow_full_day_fallback=True
                )

                # Should return a valid time window
                assert datetime.fromisoformat(start)
                assert datetime.fromisoformat(end)

    def test_allow_full_day_fallback_false_disables_recovery(self):
        """Should not trigger recovery when allow_full_day_fallback=False."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir)
            cache_dir.mkdir(exist_ok=True)

            # Create a very old cache file
            old_time = datetime.now() - timedelta(days=1)
            cache_file = cache_dir / "test_cache.json"
            cache_file.write_text("{}")

            # Set file modification time to 1 day ago
            mtime = old_time.timestamp()
            import os

            os.utime(cache_file, (mtime, mtime))

            with patch("hbnmigration.config.Config") as mock_config:
                mock_config.RECOVERY_MODE = False
                with patch(
                    "hbnmigration.utility_functions.cache.get_cache_dir"
                ) as mock_get_cache_dir:
                    mock_get_cache_dir.return_value = cache_dir

                    start, end = get_recent_time_window(
                        minutes_back=2,
                        allow_full_day_fallback=False,  # Disabled
                    )

                    start_dt = datetime.fromisoformat(start)
                    end_dt = datetime.fromisoformat(end)

                    # Window should be approximately 2 minutes (normal operation)
                    delta = end_dt - start_dt
                    assert delta.total_seconds() <= 120

    def test_custom_minutes_back_parameter(self):
        """Should respect custom minutes_back parameter."""
        with patch("hbnmigration.config.Config") as mock_config:
            mock_config.RECOVERY_MODE = False

            start, end = get_recent_time_window(
                minutes_back=5, allow_full_day_fallback=False
            )

            start_dt = datetime.fromisoformat(start)
            end_dt = datetime.fromisoformat(end)

            # Window should be approximately 5 minutes
            delta = end_dt - start_dt
            assert delta.total_seconds() <= 300  # At most 5 minutes
            assert delta.total_seconds() >= 299  # At least ~4:59


class TestDataCache:
    """Test `DataCache`s."""

    @pytest.fixture
    def cache_dir(self, tmp_path: Path) -> str:
        """Test cache directory."""
        return str(tmp_path / "test_cache")

    @pytest.fixture
    def cache(self, cache_dir: str) -> DataCache:
        """Test cache instatiation."""
        return DataCache("test_job", ttl_minutes=5, cache_dir=cache_dir)

    def test_init_creates_directory(self, cache_dir: str) -> None:
        """Test cache directory creation."""
        DataCache("test", cache_dir=cache_dir)
        assert Path(cache_dir).exists()

    def test_init_creates_cache_file(self, cache: DataCache) -> None:
        """Test cache file creation."""
        cache.mark_processed("item1")
        assert cache.cache_file.exists()

    def test_mark_and_check_processed(self, cache: DataCache) -> None:
        """Test processed metadata."""
        assert not cache.is_processed("item1")
        cache.mark_processed("item1")
        assert cache.is_processed("item1")

    def test_mark_with_metadata(self, cache: DataCache) -> None:
        """Test cache metadata."""
        cache.mark_processed("item1", metadata={"source": "test"})
        entries = cache.get_all_entries()
        assert entries["item1"]["metadata"]["source"] == "test"

    def test_expired_entries_are_not_processed(self, cache_dir: str) -> None:
        """Test expired cache entries."""
        cache = DataCache("test_expire", ttl_minutes=0, cache_dir=cache_dir)
        # Manually write an old entry
        cache._cache["old_item"] = {
            "timestamp": time.time() - 100,
            "processed_at": "2020-01-01T00:00:00",
            "metadata": {},
        }
        cache._save_cache()

        # Re-load — should clean up
        cache2 = DataCache("test_expire", ttl_minutes=0, cache_dir=cache_dir)
        assert not cache2.is_processed("old_item")

    def test_remove(self, cache: DataCache) -> None:
        """Test removing existing cache."""
        cache.mark_processed("item1")
        assert cache.is_processed("item1")
        cache.remove("item1")
        assert not cache.is_processed("item1")

    def test_remove_nonexistent(self, cache: DataCache) -> None:
        """Test removing nonexistent cache."""
        # Should not raise
        cache.remove("nonexistent")

    def test_clear(self, cache: DataCache) -> None:
        """Test clearing cache."""
        cache.mark_processed("item1")
        cache.mark_processed("item2")
        cache.clear()
        assert not cache.is_processed("item1")
        assert not cache.is_processed("item2")

    def test_get_stats(self, cache: DataCache) -> None:
        """Test getting stats."""
        cache.mark_processed("item1")
        stats = cache.get_stats()
        assert stats["cache_name"] == "test_job"
        assert stats["total_entries"] == 1
        assert stats["ttl_minutes"] == 5
        assert "file_size_bytes" in stats
        assert "oldest_entry" in stats
        assert "newest_entry" in stats

    def test_get_stats_empty(self, cache: DataCache) -> None:
        """Test empty stats."""
        stats = cache.get_stats()
        assert stats["total_entries"] == 0

    def test_get_all_entries(self, cache: DataCache) -> None:
        """Test get all cache."""
        cache.mark_processed("a")
        cache.mark_processed("b")
        entries = cache.get_all_entries()
        assert set(entries.keys()) == {"a", "b"}

    def test_persistence_across_instances(self, cache_dir: str) -> None:
        """Test persistence."""
        cache1 = DataCache("persist_test", cache_dir=cache_dir)
        cache1.mark_processed("item1")

        cache2 = DataCache("persist_test", cache_dir=cache_dir)
        assert cache2.is_processed("item1")

    def test_corrupt_cache_file(self, cache_dir: str) -> None:
        """Test corrupt cache file."""
        cache = DataCache("corrupt_test", cache_dir=cache_dir)
        # Write corrupt data
        with open(cache.cache_file, "w") as f:
            f.write("not json{{{")

        # Should handle gracefully
        cache2 = DataCache("corrupt_test", cache_dir=cache_dir)
        assert cache2.get_stats()["total_entries"] == 0

    def test_default_cache_dir_uses_env_var(self, tmp_path: Path) -> None:
        """Test env var usage."""
        test_dir = str(tmp_path / "env_cache")
        with patch.dict("os.environ", {"HBNMIGRATION_CACHE_DIR": test_dir}):
            cache = DataCache("env_test")
            assert str(cache.cache_dir) == test_dir


class TestGetRecentTimeWindow:
    """Tests for get_recent_time_window remain similar but verify no regressions."""

    @patch("hbnmigration.config.Config")
    def test_returns_tuple_of_strings(self, mock_config: Any) -> None:
        """Test return type."""
        mock_config.RECOVERY_MODE = False
        start, end = get_recent_time_window(5)
        assert isinstance(start, str)
        assert isinstance(end, str)
