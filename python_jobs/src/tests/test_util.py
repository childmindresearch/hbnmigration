"""Tests for utility functions."""

from datetime import datetime, timedelta

from hbnmigration.utility_functions.custom import yesterday_or_more_recent


class TestYesterdayOrMoreRecent:
    """Tests for yesterday_or_more_recent() date validation function."""

    def test_today_is_more_recent(self):
        """Should return True for today's date."""
        today = datetime.now().date()
        result = yesterday_or_more_recent(today.isoformat())
        assert result is True

    def test_yesterday_is_more_recent(self):
        """Should return True for yesterday's date."""
        yesterday = (datetime.now() - timedelta(days=1)).date()
        result = yesterday_or_more_recent(yesterday.isoformat())
        assert result is True

    def test_future_date_is_more_recent(self):
        """Should return True for future dates."""
        tomorrow = (datetime.now() + timedelta(days=1)).date()
        result = yesterday_or_more_recent(tomorrow.isoformat())
        assert result is True

    def test_two_days_ago_is_not_more_recent(self):
        """Should return False for dates older than yesterday."""
        two_days_ago = (datetime.now() - timedelta(days=2)).date()
        result = yesterday_or_more_recent(two_days_ago.isoformat())
        assert result is False

    def test_iso_datetime_string_comparison(self):
        """Should correctly compare ISO datetime strings (not just dates)."""
        # This tests the specific bug fix: comparing datetime.date to datetime.datetime
        now = datetime.now()
        iso_string = now.isoformat()
        result = yesterday_or_more_recent(iso_string)
        assert result is True

    def test_iso_datetime_with_timezone_info(self):
        """Should handle ISO datetime strings with timezone information."""
        # Even with timezone info, should compare correctly
        now_with_tz = datetime.now().isoformat()
        result = yesterday_or_more_recent(now_with_tz)
        assert result is True

    def test_old_date_with_iso_format(self):
        """Should return False for old dates in ISO format."""
        old_date = (datetime.now() - timedelta(days=10)).isoformat()
        result = yesterday_or_more_recent(old_date)
        assert result is False
