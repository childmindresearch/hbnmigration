# python_jobs/src/tests/test_track_curious.py
"""Tests for hbnmigration.from_redcap.track_curious."""

import logging

from _pytest.logging import LogCaptureFixture
import pandas as pd

from hbnmigration.from_redcap.track_curious import (
    filter_existing,
    get_tracked,
    rename_fields,
    subset_complete_data,
)


class TestGetTracked:
    """Tests for get_tracked()."""

    def test_returns_fields_with_matching_suffix(self) -> None:
        """Test matching."""
        metadata = pd.DataFrame(
            {"field_name": ["asr_sr_received", "cbcl_p_received", "record_id"]}
        )
        result = get_tracked(metadata, "_received")
        assert result == ["asr_sr_received", "cbcl_p_received"]

    def test_returns_empty_list_when_no_matches(self) -> None:
        """Test no matches."""
        metadata = pd.DataFrame({"field_name": ["record_id", "mrn", "dob"]})
        result = get_tracked(metadata, "_received")
        assert result == []

    def test_does_not_match_partial_suffix(self) -> None:
        """Test partial suffix."""
        metadata = pd.DataFrame({"field_name": ["not_received_yet", "asr_sr_received"]})
        result = get_tracked(metadata, "_received")
        assert result == ["asr_sr_received"]

    def test_deduplicates_via_unique(self) -> None:
        """Test unique."""
        metadata = pd.DataFrame(
            {"field_name": ["asr_sr_received", "asr_sr_received", "cbcl_p_received"]}
        )
        result = get_tracked(metadata, "_received")
        assert result == ["asr_sr_received", "cbcl_p_received"]


class TestSubsetCompleteData:
    """Tests for subset_complete_data()."""

    def test_filters_to_complete_fields_with_value_2(self) -> None:
        """Test filters."""
        data = pd.DataFrame(
            {
                "record": ["1", "1", "2"],
                "field_name": [
                    "asr_sr_18_complete",
                    "cbcl_p_complete",
                    "asr_sr_18_complete",
                ],
                "value": ["2", "2", "0"],
                "redcap_event_name": ["event_1", "event_1", "event_1"],
            }
        )
        result = subset_complete_data(data, "complete")
        assert len(result) == 2
        assert all(result["value"] == "1")
        assert all(result["redcap_event_name"] == "admin_arm_1")

    def test_excludes_non_complete_fields(self) -> None:
        """Test not complete."""
        data = pd.DataFrame(
            {
                "record": ["1"],
                "field_name": ["asr_sr_18_timestamp"],
                "value": ["2"],
                "redcap_event_name": ["event_1"],
            }
        )
        result = subset_complete_data(data, "complete")
        assert result.empty

    def test_excludes_incomplete_values(self) -> None:
        """Test incomplete."""
        data = pd.DataFrame(
            {
                "record": ["1", "2", "3"],
                "field_name": [
                    "asr_sr_complete",
                    "asr_sr_complete",
                    "asr_sr_complete",
                ],
                "value": ["0", "1", ""],
                "redcap_event_name": ["event_1", "event_1", "event_1"],
            }
        )
        result = subset_complete_data(data, "complete")
        assert result.empty

    def test_handles_non_numeric_values(self) -> None:
        """Test nan."""
        data = pd.DataFrame(
            {
                "record": ["1", "2"],
                "field_name": ["asr_sr_complete", "asr_sr_complete"],
                "value": ["N/A", "2"],
                "redcap_event_name": ["event_1", "event_1"],
            }
        )
        result = subset_complete_data(data, "complete")
        assert len(result) == 1
        assert result.iloc[0]["record"] == "2"


class TestFilterExisting:
    """Tests for filter_existing()."""

    def test_removes_rows_present_in_both(self) -> None:
        """Test dedupe."""
        new_data = pd.DataFrame(
            {
                "record": ["1", "2", "3"],
                "field_name": ["asr_sr_received", "cbcl_p_received", "asr_sr_received"],
                "value": ["1", "1", "1"],
                "redcap_event_name": ["admin_arm_1", "admin_arm_1", "admin_arm_1"],
            }
        )
        existing_data = pd.DataFrame(
            {
                "record": ["1"],
                "field_name": ["asr_sr_received"],
                "value": ["1"],
                "redcap_event_name": ["admin_arm_1"],
            }
        )
        result = filter_existing(new_data, existing_data)
        assert len(result) == 2
        assert (
            "1" not in result["record"].values
            or not (
                (result["record"] == "1") & (result["field_name"] == "asr_sr_received")
            ).any()
        )

    def test_returns_all_when_no_overlap(self) -> None:
        """Test no overlap."""
        new_data = pd.DataFrame(
            {
                "record": ["1", "2"],
                "field_name": ["asr_sr_received", "cbcl_p_received"],
                "value": ["1", "1"],
                "redcap_event_name": ["admin_arm_1", "admin_arm_1"],
            }
        )
        existing_data = pd.DataFrame(
            {
                "record": ["3"],
                "field_name": ["asr_sr_received"],
                "value": ["1"],
                "redcap_event_name": ["admin_arm_1"],
            }
        )
        result = filter_existing(new_data, existing_data)
        assert len(result) == 2

    def test_returns_empty_when_all_exist(self) -> None:
        """Test all already exist."""
        data = pd.DataFrame(
            {
                "record": ["1"],
                "field_name": ["asr_sr_received"],
                "value": ["1"],
                "redcap_event_name": ["admin_arm_1"],
            }
        )
        result = filter_existing(data, data.copy())
        assert result.empty

    def test_handles_empty_new_data(self) -> None:
        """Test empty new data."""
        new_data = pd.DataFrame(
            columns=["record", "field_name", "value", "redcap_event_name"]
        )
        existing_data = pd.DataFrame(
            {
                "record": ["1"],
                "field_name": ["asr_sr_received"],
                "value": ["1"],
                "redcap_event_name": ["admin_arm_1"],
            }
        )
        result = filter_existing(new_data, existing_data)
        assert result.empty

    def test_handles_empty_existing_data(self) -> None:
        """Test empty existing data."""
        new_data = pd.DataFrame(
            {
                "record": ["1", "2"],
                "field_name": ["asr_sr_received", "cbcl_p_received"],
                "value": ["1", "1"],
                "redcap_event_name": ["admin_arm_1", "admin_arm_1"],
            }
        )
        existing_data = pd.DataFrame(
            columns=["record", "field_name", "value", "redcap_event_name"]
        )
        result = filter_existing(new_data, existing_data)
        assert len(result) == 2


class TestRenameFields:
    """Tests for rename_fields()."""

    def test_renames_complete_to_received(self) -> None:
        """Test rename."""
        new_df = pd.DataFrame(
            {
                "record": ["1", "2"],
                "field_name": ["asr_sr_18_complete", "cbcl_p_complete"],
                "value": ["1", "1"],
                "redcap_event_name": ["admin_arm_1", "admin_arm_1"],
            }
        )
        track_metadata = pd.DataFrame(
            {"field_name": ["asr_sr_received", "cbcl_p_received"]}
        )
        result = rename_fields(new_df, track_metadata)
        assert list(result["field_name"]) == ["asr_sr_received", "cbcl_p_received"]

    def test_strips_age_number_before_matching(self) -> None:
        """Test matching pattern."""
        new_df = pd.DataFrame(
            {
                "record": ["1"],
                "field_name": ["amas_sr_14_complete"],
                "value": ["1"],
                "redcap_event_name": ["admin_arm_1"],
            }
        )
        track_metadata = pd.DataFrame({"field_name": ["amas_sr_received"]})
        result = rename_fields(new_df, track_metadata)
        assert result.iloc[0]["field_name"] == "amas_sr_received"

    def test_drops_unmatched_rows_and_logs_warning(
        self, caplog: LogCaptureFixture
    ) -> None:
        """Test unmatched rows."""
        new_df = pd.DataFrame(
            {
                "record": ["1", "2"],
                "field_name": ["asr_sr_complete", "unknown_instrument_complete"],
                "value": ["1", "1"],
                "redcap_event_name": ["admin_arm_1", "admin_arm_1"],
            }
        )
        track_metadata = pd.DataFrame({"field_name": ["asr_sr_received"]})
        with caplog.at_level(logging.WARNING):
            result = rename_fields(new_df, track_metadata)
        assert len(result) == 1
        assert result.iloc[0]["field_name"] == "asr_sr_received"
        assert "unknown_instrument_complete" in caplog.text

    def test_returns_empty_when_no_matches(self, caplog: LogCaptureFixture) -> None:
        """Test no matches."""
        new_df = pd.DataFrame(
            {
                "record": ["1"],
                "field_name": ["unknown_complete"],
                "value": ["1"],
                "redcap_event_name": ["admin_arm_1"],
            }
        )
        track_metadata = pd.DataFrame({"field_name": ["asr_sr_received"]})
        with caplog.at_level(logging.WARNING):
            result = rename_fields(new_df, track_metadata)
        assert result.empty
        assert "unknown_complete" in caplog.text

    def test_no_instrument_helper_column_in_output(self) -> None:
        """Test no instrument."""
        new_df = pd.DataFrame(
            {
                "record": ["1"],
                "field_name": ["asr_sr_complete"],
                "value": ["1"],
                "redcap_event_name": ["admin_arm_1"],
            }
        )
        track_metadata = pd.DataFrame({"field_name": ["asr_sr_received"]})
        result = rename_fields(new_df, track_metadata)
        assert "instrument" not in result.columns

    def test_handles_empty_dataframe(self) -> None:
        """Test empty DataFrame handling."""
        new_df = pd.DataFrame(
            columns=["record", "field_name", "value", "redcap_event_name"]
        )
        track_metadata = pd.DataFrame({"field_name": ["asr_sr_received"]})
        result = rename_fields(new_df, track_metadata)
        assert result.empty
