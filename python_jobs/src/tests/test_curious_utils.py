"""Tests for curious utility functions."""

from unittest.mock import Mock, patch

import pandas as pd
import polars as pl
import pytest
import requests

from hbnmigration.from_curious.utils import parse_dt
from hbnmigration.utility_functions.custom import (
    fetch_api_data,
    fetch_api_data1,
)

# ============================================================================
# Tests - parse_dt
# ============================================================================


class TestParseDt:
    """Tests for the parse_dt datetime parser."""

    @pytest.mark.parametrize(
        "dt_str",
        [
            "2024-01-01T12:00:00.000Z",
            "2024-01-01T12:00:00.000",
            "2024-01-01T12:00:00.000000",
            "2026-04-24T16:36:42.042000",
            "2024-12-31T23:59:59.999Z",
            "2024-06-15T08:30:00.123456",
        ],
        ids=[
            "3_frac_with_Z",
            "3_frac_no_Z",
            "6_frac_no_Z",
            "6_frac_no_Z_future",
            "3_frac_with_Z_end_of_day",
            "6_frac_no_Z_mid_day",
        ],
    )
    def test_parses_various_iso_formats(self, dt_str: str) -> None:
        """parse_dt should handle 3- or 6-digit fractional seconds, with/out Z."""
        df = pl.DataFrame({"ts": [dt_str]})
        result = df.select(parse_dt("ts").alias("parsed"))
        col = result["parsed"]
        assert col.dtype == pl.Datetime("ms", "UTC")
        assert col.null_count() == 0

    def test_parses_both_formats_in_same_column(self) -> None:
        """A single column mixing trailing-Z and no-Z strings should parse fully."""
        df = pl.DataFrame(
            {
                "ts": [
                    "2024-01-01T12:00:00.000Z",
                    "2026-04-24T16:36:42.042000",
                ]
            }
        )
        result = df.select(parse_dt("ts").alias("parsed"))
        assert result["parsed"].null_count() == 0

    def test_preserves_datetime_value(self) -> None:
        """Parsed datetime should have the expected timestamp value."""
        df = pl.DataFrame({"ts": ["2024-06-15T10:30:00.500Z"]})
        result = df.select(parse_dt("ts").alias("parsed"))
        dt = result["parsed"][0]
        assert dt.year == 2024
        assert dt.month == 6
        assert dt.day == 15
        assert dt.hour == 10
        assert dt.minute == 30
        assert dt.second == 0

    def test_result_is_utc(self) -> None:
        """Parsed datetime should always be UTC."""
        df = pl.DataFrame({"ts": ["2024-01-01T00:00:00.000"]})
        result = df.select(parse_dt("ts").alias("parsed"))
        assert result["parsed"].dtype == pl.Datetime("ms", "UTC")

    def test_multiple_rows(self) -> None:
        """parse_dt should work across multiple rows without error."""
        df = pl.DataFrame(
            {
                "ts": [
                    "2024-01-01T00:00:00.000Z",
                    "2024-06-15T12:30:00.123456",
                    "2026-04-24T16:36:42.042000",
                ]
            }
        )
        result = df.select(parse_dt("ts").alias("parsed"))
        assert len(result) == 3
        assert result["parsed"].null_count() == 0


# ============================================================================
# Tests - fetch_api_data
# ============================================================================


def test_fetch_api_data_sends_teams_alert_on_new_invalid_fields(tmp_path, monkeypatch):
    """Test that new invalid fields trigger Teams alert via fetch_api_data."""
    monkeypatch.setattr(
        "hbnmigration.utility_functions.logging.log_root_path", lambda: tmp_path
    )
    with (
        patch("hbnmigration.utility_functions.custom.requests.post") as mock_post,
        patch("hbnmigration.utility_functions.custom.send_alert") as mock_alert,
        patch(
            "hbnmigration.utility_functions.custom._redcap_project_info"
        ) as mock_info,
    ):
        # Setup mock for invalid fields response
        mock_response = Mock()
        mock_response.status_code = requests.codes["bad"]
        # Important: Include the exact string that the code looks for
        mock_response.text = (
            'ERROR: The following values in the parameter "fields" are not valid: '
            '"field1", "field2"'  # Note: space after comma
        )
        mock_post.return_value = mock_response
        # Setup project info
        mock_info.return_value = ("Test Project", "123")
        # Call public fetch_api_data
        result = fetch_api_data(
            "https://redcap.test/api/",
            {},
            {"token": "test_token", "fields": "field1,field2"},
        )
        # Should return empty DataFrame on error
        assert isinstance(result, pd.DataFrame)
        assert result.empty
        # Should have sent alert for new invalid fields
        assert mock_alert.called
        alert_message = mock_alert.call_args[0][0]
        assert "Test Project" in alert_message
        assert "PID 123" in alert_message


def test_fetch_api_data1_no_alert_for_duplicate_invalid_fields(tmp_path, monkeypatch):
    """Test that duplicate invalid fields don't trigger alert via fetch_api_data1."""
    monkeypatch.setattr(
        "hbnmigration.utility_functions.logging.log_root_path", lambda: tmp_path
    )
    with (
        patch("hbnmigration.utility_functions.custom.requests.post") as mock_post,
        patch("hbnmigration.utility_functions.custom.send_alert") as mock_alert,
        patch("hbnmigration.utility_functions.custom.log_invalid_fields") as mock_log,
    ):
        mock_response = Mock()
        mock_response.status_code = requests.codes["bad"]
        mock_response.text = (
            'ERROR: The following values in the parameter "fields" are not valid: '
            '"field1"'
        )
        mock_post.return_value = mock_response
        # Simulate that field was already logged
        mock_log.return_value = []  # No new fields
        result = fetch_api_data1(
            "https://redcap.test/api/",
            {},
            {"token": "test_token", "fields": "field1"},
        )
        # Should return None for empty result
        assert result is None
        # Should NOT send alert since no new fields
        assert not mock_alert.called


def test_fetch_api_data_successful_returns_dataframe():
    """Test that successful fetch returns DataFrame."""
    with (
        patch("hbnmigration.utility_functions.custom.requests.post") as mock_post,
        patch("hbnmigration.utility_functions.custom.send_alert") as mock_alert,
    ):
        mock_response = Mock()
        mock_response.status_code = requests.codes["okay"]
        mock_response.text = "record_id,field1\n001,value1\n002,value2\n"
        mock_post.return_value = mock_response
        result = fetch_api_data(
            "https://redcap.test/api/",
            {},
            {"token": "test_token", "fields": "field1"},
        )
        # Should return DataFrame with data
        assert isinstance(result, pd.DataFrame)
        assert not result.empty
        assert "record_id" in result.columns
        assert "field1" in result.columns
        # Should NOT send alert on success
        assert not mock_alert.called


def test_fetch_api_data_returns_list_when_requested():
    """Test that fetch_api_data can return a list when specified."""
    with patch("hbnmigration.utility_functions.custom.requests.post") as mock_post:
        mock_response = Mock()
        mock_response.status_code = requests.codes["okay"]
        # Use strings that won't be converted to int
        mock_response.text = "record,field1\nAAA,value1\nBBB,value2\n"
        mock_post.return_value = mock_response
        result = fetch_api_data(
            "https://redcap.test/api/",
            {},
            {"token": "test_token"},
            return_type=list,
            column="record",
        )
        # Should return list of records
        assert isinstance(result, list)
        assert "AAA" in result
        assert "BBB" in result
