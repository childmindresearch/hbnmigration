"""Tests for cache recovery mode functionality."""

from datetime import datetime, timedelta
from pathlib import Path
import tempfile
from unittest.mock import patch

from hbnmigration.utility_functions.cache import get_recent_time_window


class TestGetRecentTimeWindowRecoveryMode:
    """Tests for get_recent_time_window recovery mode behavior."""

    def test_normal_operation_returns_2_minute_window(self):
        """Should return 2-minute window during normal operation."""
        with patch("hbnmigration.utility_functions.cache.Config") as mock_config:
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
        with patch("hbnmigration.utility_functions.cache.Config") as mock_config:
            mock_config.RECOVERY_MODE = True

            start, end = get_recent_time_window(
                minutes_back=2, allow_full_day_fallback=True
            )

            start_dt = datetime.fromisoformat(start)
            end_dt = datetime.fromisoformat(end)

            # Window should be approximately 1 day
            delta = end_dt - start_dt
            assert delta.total_seconds() >= 86400  # At least 24 hours
            assert delta.total_seconds() < 86401  # Less than 24 hours + 1 second

    def test_recovery_mode_logs_warning(self, caplog):
        """Should log warning when recovery mode is enabled."""
        with patch("hbnmigration.utility_functions.cache.Config") as mock_config:
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

            with patch("hbnmigration.utility_functions.cache.Config") as mock_config:
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

            with patch("hbnmigration.utility_functions.cache.Config") as mock_config:
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

            with patch("hbnmigration.utility_functions.cache.Config") as mock_config:
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
        with patch("hbnmigration.utility_functions.cache.Config") as mock_config:
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

            with patch("hbnmigration.utility_functions.cache.Config") as mock_config:
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
        with patch("hbnmigration.utility_functions.cache.Config") as mock_config:
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
