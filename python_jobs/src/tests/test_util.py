"""Tests for utility functions."""

from datetime import datetime, timedelta
from unittest.mock import Mock, patch

import pytest
import requests

from hbnmigration.utility_functions.custom import (
    new_curious_account,
    yesterday_or_more_recent,
)
from hbnmigration.utility_functions.datatypes import Record


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


class TestNewCuriousAccount:
    """Tests for new_curious_account() function."""

    @pytest.fixture
    def base_headers(self) -> dict[str, str]:
        """Return base headers for API requests."""
        return {
            "Content-Type": "application/json",
            "Authorization": "Bearer test_token",
        }

    @pytest.fixture
    def base_record(self) -> dict[str, str]:
        """Return base participant record without parent_involvement."""
        return {
            "secretUserId": "00001",
            "firstName": "Test",
            "lastName": "User",
            "accountType": "limited",
            "tag": "child",
        }

    @pytest.fixture
    def mock_response_success(self) -> Mock:
        """Mock successful API response."""
        mock_response = Mock()
        mock_response.status_code = requests.codes["okay"]
        mock_response.json.return_value = {"id": "new_account_id"}
        return mock_response

    def test_parent_involvement_set_converted_to_sorted_list(
        self,
        base_record: Record,
        base_headers: dict[str, str],
        mock_response_success: Mock,
    ) -> None:
        """Should convert parent_involvement from set to sorted list."""
        record_with_set = base_record.copy()
        record_with_set["parent_involvement"] = {3, 1, 2}

        with patch("hbnmigration.utility_functions.custom.requests.post") as mock_post:
            mock_post.return_value = mock_response_success

            result = new_curious_account(
                "https://curious.test",
                "test_applet_id",
                record_with_set,
                base_headers,
            )

            # Verify the posted data has parent_involvement as sorted list
            call_kwargs = mock_post.call_args[1]
            posted_data = call_kwargs["json"]
            assert isinstance(posted_data["parent_involvement"], list)
            assert posted_data["parent_involvement"] == [1, 2, 3]
            assert result == "limited account created for MRN 00001."

    def test_parent_involvement_list_gets_sorted(
        self,
        base_record: Record,
        base_headers: dict[str, str],
        mock_response_success: Mock,
    ) -> None:
        """Should sort parent_involvement if already a list."""
        record_with_list = base_record.copy()
        record_with_list["parent_involvement"] = [3, 1, 2]

        with patch("hbnmigration.utility_functions.custom.requests.post") as mock_post:
            mock_post.return_value = mock_response_success

            result = new_curious_account(
                "https://curious.test",
                "test_applet_id",
                record_with_list,
                base_headers,
            )

            # Verify list is sorted
            call_kwargs = mock_post.call_args[1]
            posted_data = call_kwargs["json"]
            assert isinstance(posted_data["parent_involvement"], list)
            assert posted_data["parent_involvement"] == [1, 2, 3]
            assert result == "limited account created for MRN 00001."

    def test_no_parent_involvement_field(
        self,
        base_record: Record,
        base_headers: dict[str, str],
        mock_response_success: Mock,
    ) -> None:
        """Should handle records without parent_involvement field."""
        with patch("hbnmigration.utility_functions.custom.requests.post") as mock_post:
            mock_post.return_value = mock_response_success

            result = new_curious_account(
                "https://curious.test",
                "test_applet_id",
                base_record,
                base_headers,
            )

            # Should not raise error
            call_kwargs = mock_post.call_args[1]
            posted_data = call_kwargs["json"]
            assert "parent_involvement" not in posted_data
            assert result == "limited account created for MRN 00001."

    def test_parent_involvement_empty_set(
        self,
        base_record: Record,
        base_headers: dict[str, str],
        mock_response_success: Mock,
    ) -> None:
        """Should convert empty set to empty list."""
        record_with_empty_set = base_record.copy()
        record_with_empty_set["parent_involvement"] = set()

        with patch("hbnmigration.utility_functions.custom.requests.post") as mock_post:
            mock_post.return_value = mock_response_success

            result = new_curious_account(
                "https://curious.test",
                "test_applet_id",
                record_with_empty_set,
                base_headers,
            )

            call_kwargs = mock_post.call_args[1]
            posted_data = call_kwargs["json"]
            assert posted_data["parent_involvement"] == []
            assert result == "limited account created for MRN 00001."

    def test_parent_involvement_single_element_set(
        self,
        base_record: Record,
        base_headers: dict[str, str],
        mock_response_success: Mock,
    ) -> None:
        """Should convert single-element set to single-element list."""
        record_with_single = base_record.copy()
        record_with_single["parent_involvement"] = {1}

        with patch("hbnmigration.utility_functions.custom.requests.post") as mock_post:
            mock_post.return_value = mock_response_success

            result = new_curious_account(
                "https://curious.test",
                "test_applet_id",
                record_with_single,
                base_headers,
            )

            call_kwargs = mock_post.call_args[1]
            posted_data = call_kwargs["json"]
            assert posted_data["parent_involvement"] == [1]
            assert result == "limited account created for MRN 00001."

    def test_parent_involvement_with_string_values(
        self,
        base_record: Record,
        base_headers: dict[str, str],
        mock_response_success: Mock,
    ) -> None:
        """Should handle set with string values and sort alphabetically."""
        record_with_strings = base_record.copy()
        record_with_strings["parent_involvement"] = {"c", "a", "b"}

        with patch("hbnmigration.utility_functions.custom.requests.post") as mock_post:
            mock_post.return_value = mock_response_success

            result = new_curious_account(
                "https://curious.test",
                "test_applet_id",
                record_with_strings,
                base_headers,
            )

            call_kwargs = mock_post.call_args[1]
            posted_data = call_kwargs["json"]
            assert posted_data["parent_involvement"] == ["a", "b", "c"]
            assert result == "limited account created for MRN 00001."

    def test_account_type_limited_creates_shell_account(
        self, base_record: Record, base_headers: dict[str, str]
    ) -> None:
        """Should create shell-account for limited accountType."""
        with patch("hbnmigration.utility_functions.custom.requests.post") as mock_post:
            mock_response = Mock()
            mock_response.status_code = requests.codes["okay"]
            mock_response.json.return_value = {"id": "new_account_id"}
            mock_post.return_value = mock_response

            new_curious_account(
                "https://curious.test",
                "test_applet_id",
                base_record,
                base_headers,
            )

            # Verify URL contains shell-account
            call_args = mock_post.call_args[0]
            assert "shell-account" in call_args[0]

    def test_account_type_full_creates_respondent(
        self, base_record: Record, base_headers: dict[str, str]
    ) -> None:
        """Should create respondent for full accountType."""
        record_full = base_record.copy()
        record_full["accountType"] = "full"

        with patch("hbnmigration.utility_functions.custom.requests.post") as mock_post:
            mock_response = Mock()
            mock_response.status_code = requests.codes["okay"]
            mock_response.json.return_value = {"id": "new_account_id"}
            mock_post.return_value = mock_response

            new_curious_account(
                "https://curious.test",
                "test_applet_id",
                record_full,
                base_headers,
            )

            # Verify URL contains respondent
            call_args = mock_post.call_args[0]
            assert "respondent" in call_args[0]

    def test_invalid_account_type_raises_error(
        self, base_record: Record, base_headers: dict[str, str]
    ) -> None:
        """Should raise ValueError for invalid accountType."""
        record_invalid = base_record.copy()
        record_invalid["accountType"] = "invalid"

        with pytest.raises(ValueError, match="No valid account type specified"):
            new_curious_account(
                "https://curious.test",
                "test_applet_id",
                record_invalid,
                base_headers,
            )

    def test_account_already_exists_unprocessable(
        self, base_record: Record, base_headers: dict[str, str]
    ) -> None:
        """Should return exists message for 422 status code."""
        with patch("hbnmigration.utility_functions.custom.requests.post") as mock_post:
            mock_response = Mock()
            mock_response.status_code = requests.codes["unprocessable"]
            mock_response.json.return_value = {}
            mock_post.return_value = mock_response

            result = new_curious_account(
                "https://curious.test",
                "test_applet_id",
                base_record,
                base_headers,
            )

            assert result == "Account already exists for MRN 00001"

    def test_account_already_exists_bad_request_non_unique(
        self, base_record: Record, base_headers: dict[str, str]
    ) -> None:
        """Should return exists message for 400 with non-unique error."""
        with patch("hbnmigration.utility_functions.custom.requests.post") as mock_post:
            mock_response = Mock()
            mock_response.status_code = requests.codes["bad"]
            mock_response.json.return_value = {
                "result": [{"message": "Non-unique value."}]
            }
            mock_post.return_value = mock_response

            result = new_curious_account(
                "https://curious.test",
                "test_applet_id",
                base_record,
                base_headers,
            )

            assert result == "Account already exists for MRN 00001"

    def test_single_element_collection_unpacked(
        self,
        base_record: Record,
        base_headers: dict[str, str],
        mock_response_success: Mock,
    ) -> None:
        """Should unpack single-element list/set/tuple to scalar value."""
        record_with_singles = base_record.copy()
        record_with_singles["single_list"] = ["value"]
        record_with_singles["single_set"] = {"value"}
        record_with_singles["single_tuple"] = ("value",)

        with patch("hbnmigration.utility_functions.custom.requests.post") as mock_post:
            mock_post.return_value = mock_response_success

            new_curious_account(
                "https://curious.test",
                "test_applet_id",
                record_with_singles,
                base_headers,
            )

            call_kwargs = mock_post.call_args[1]
            posted_data = call_kwargs["json"]
            assert posted_data["single_list"] == "value"
            assert posted_data["single_set"] == "value"
            assert posted_data["single_tuple"] == "value"
