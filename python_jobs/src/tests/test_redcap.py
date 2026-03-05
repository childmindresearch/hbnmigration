"""Test code for data transfer from REDCap."""

from unittest.mock import patch

import pandas as pd
import pytest

from hbnmigration.exceptions import NoData
from hbnmigration.from_redcap import to_redcap as transfer_module
from hbnmigration.from_redcap.config import Fields, Values

from .conftest import (
    assert_field_renamed,
    assert_permission_decremented,
    assert_redcap_eav_structure,
    calculate_total_eav_rows,
    count_records_in_eav,
    get_unique_field_values,
    patch_redcap_fetch_dependencies,
    patch_redcap_transfer_module,
)

# ============================================================================
# fetch_data() Tests
# ============================================================================


class TestFetchData:
    """Tests for fetch_data function."""

    def test_fetch_data_success(self, sample_redcap_data):
        """Test successful data fetch from REDCap."""
        with patch_redcap_fetch_dependencies(
            fetch_api_return=sample_redcap_data.copy(),
        ) as mocks:
            mocks["endpoints"].return_value.base_url = "https://redcap.test/api/"
            mocks["redcap_vars"].headers = {}

            # Execute
            result = transfer_module.fetch_data("test_token", "intake_ready,consent1")

            # Assert
            assert isinstance(result, pd.DataFrame)
            assert len(result) == len(sample_redcap_data)
            assert_redcap_eav_structure(result)
            mocks["fetch_api"].assert_called_once()

    def test_fetch_data_no_data_raises_exception(self, empty_redcap_data):
        """Test that NoData exception is raised when no records returned."""
        with patch_redcap_fetch_dependencies(fetch_api_return=empty_redcap_data):
            with pytest.raises(NoData):
                transfer_module.fetch_data("test_token", "intake_ready")

    def test_fetch_data_applies_field_rename(self):
        """Test that field names are renamed according to configuration."""
        # Use actual field names from config
        original_field = "consent1"
        renamed_field = Fields.rename_247_to_744[original_field]

        data = pd.DataFrame(
            {
                "record": ["001"],
                "field_name": [original_field],
                "value": ["Test"],
                "redcap_repeat_instrument": [""],
                "redcap_repeat_instance": [""],
            }
        )

        with patch_redcap_fetch_dependencies(fetch_api_return=data.copy()):
            # Execute
            result = transfer_module.fetch_data("test_token", "intake_ready")

            # Assert
            assert_field_renamed(result, original_field, renamed_field)

    def test_fetch_data_includes_filter_logic(self, sample_redcap_data):
        """Test that API call includes correct filter logic."""
        with patch_redcap_fetch_dependencies(
            fetch_api_return=sample_redcap_data
        ) as mocks:
            # Execute
            transfer_module.fetch_data("test_token", "intake_ready")

            # Assert
            call_args = mocks["fetch_api"].call_args[0][2]
            assert call_args["filterLogic"] == (
                "[intake_ready] = "
                f"{Values.PID247.intake_ready['Ready to Send to Intake Redcap']}"
            )
            assert call_args["type"] == "eav"

    def test_fetch_data_eav_format_parameters(self, sample_redcap_data):
        """Test that fetch_data requests EAV format with correct parameters."""
        with patch_redcap_fetch_dependencies(
            fetch_api_return=sample_redcap_data
        ) as mocks:
            # Execute
            transfer_module.fetch_data("test_token", "intake_ready")

            # Assert
            call_args = mocks["fetch_api"].call_args[0][2]
            assert call_args["type"] == "eav"
            assert call_args["format"] == "csv"
            assert call_args["rawOrLabel"] == "raw"


# ============================================================================
# update_source() Tests
# ============================================================================


class TestUpdateSource:
    """Tests for update_source function."""

    @patch("hbnmigration.from_redcap.to_redcap.redcap_api_push")
    @patch("hbnmigration.from_redcap.to_redcap.redcap_variables")
    @patch("hbnmigration.from_redcap.to_redcap.Endpoints")
    def test_update_source_success(
        self, mock_endpoints, mock_redcap_vars, mock_push, sample_redcap_data
    ):
        """Test successful update of source project."""
        # Setup
        expected_records = count_records_in_eav(sample_redcap_data)
        mock_push.return_value = expected_records
        mock_redcap_vars.Tokens.pid247 = "token_247"
        mock_endpoints.return_value.base_url = "https://redcap.test/api/"

        # Execute
        result = transfer_module.update_source(sample_redcap_data)

        # Assert
        assert result == expected_records
        mock_push.assert_called_once()

        # Check DataFrame passed to push
        call_df = mock_push.call_args[1]["df"]
        assert all(call_df["field_name"] == "intake_ready")
        assert all(
            call_df["value"]
            == Values.PID247.intake_ready[
                "Participant information already sent to HBN - Intake Redcap project"
            ]
        )

    @patch("hbnmigration.from_redcap.to_redcap.redcap_api_push")
    def test_update_source_creates_correct_dataframe(self, mock_push):
        """Test that update creates correct DataFrame structure."""
        # Setup
        input_df = pd.DataFrame(
            {
                "record": ["001", "001", "002", "002", "003", "003"],
                "field_name": ["name", "age", "name", "age", "name", "age"],
                "value": ["Alice", "30", "Bob", "25", "Carol", "35"],
            }
        )
        expected_num_records = count_records_in_eav(input_df)
        mock_push.return_value = expected_num_records

        # Execute
        transfer_module.update_source(input_df)

        # Assert
        call_df = mock_push.call_args[1]["df"]
        assert len(call_df) == expected_num_records
        assert list(call_df["record"].unique()) == ["001", "002", "003"]
        assert all(
            call_df["value"]
            == Values.PID247.intake_ready[
                "Participant information already sent to HBN - Intake Redcap project"
            ]
        )

    @patch("hbnmigration.from_redcap.to_redcap.redcap_api_push")
    @patch("hbnmigration.from_redcap.to_redcap.redcap_variables")
    def test_update_source_uses_correct_token(self, mock_redcap_vars, mock_push):
        """Test that update_source uses PID 247 token."""
        # Setup
        expected_token = "special_token_247"
        mock_push.return_value = 1
        mock_redcap_vars.Tokens.pid247 = expected_token
        input_df = pd.DataFrame(
            {
                "record": ["001"],
                "field_name": ["test"],
                "value": ["value"],
            }
        )

        # Execute
        transfer_module.update_source(input_df)

        # Assert
        assert mock_push.call_args[1]["token"] == expected_token


# ============================================================================
# main() Tests
# ============================================================================


class TestMain:
    """Tests for main workflow function."""

    def test_main_successful_transfer(self):
        """Test successful end-to-end data transfer."""
        # Use real field names from config (BEFORE rename)
        data = pd.DataFrame(
            {
                "record": ["001", "001"],
                "field_name": ["consent1", "permission_collab"],
                "value": ["Test", Values.PID744.permission_collab["No"]],
                "redcap_repeat_instrument": ["", ""],
                "redcap_repeat_instance": ["", ""],
            }
        )
        expected_rows = calculate_total_eav_rows(data)

        with patch_redcap_transfer_module(
            fetch_return=data.copy(),
            push_return=expected_rows,
            update_return=expected_rows,
        ) as mocks:
            # Execute
            transfer_module.main()

            # Assert
            mocks["fetch"].assert_called_once()
            mocks["push"].assert_called_once()
            mocks["update"].assert_called_once()

    def test_main_raises_nodata_when_fetch_empty(self, empty_redcap_data):
        """Test that NoData is raised when fetch returns empty DataFrame."""
        with patch_redcap_transfer_module(fetch_return=empty_redcap_data):
            with pytest.raises(NoData):
                transfer_module.main()

    def test_main_raises_nodata_when_push_fails(self):
        """Test that NoData is raised when push returns 0."""
        data = pd.DataFrame(
            {
                "record": ["001"],
                "field_name": ["consent1"],
                "value": ["Test"],
                "redcap_repeat_instrument": [""],
                "redcap_repeat_instance": [""],
            }
        )

        with patch_redcap_transfer_module(
            fetch_return=data,
            push_return=0,
        ) as mocks:
            with pytest.raises(NoData):
                transfer_module.main()

            mocks["update"].assert_not_called()

    def test_main_assertion_mismatch_raises_error(self):
        """Test that assertion error raised when row counts don't match."""
        data = pd.DataFrame(
            {
                "record": ["001"],
                "field_name": ["consent1"],
                "value": ["Test"],
                "redcap_repeat_instrument": [""],
                "redcap_repeat_instance": [""],
            }
        )

        with patch_redcap_transfer_module(
            fetch_return=data,
            push_return=2,
            update_return=3,  # Mismatch with push_return!
        ):
            with pytest.raises(AssertionError, match="rows imported to PID 744"):
                transfer_module.main()

    def test_main_filters_Fields_for_744(self) -> None:  # noqa: N802
        """Test that only appropriate Fields are sent to PID 744."""
        # Data with both matching and non-matching Fields (ORIGINAL names before rename)
        included_field = "consent1"
        excluded_field = "intake_ready"

        data = pd.DataFrame(
            {
                "record": ["001", "001", "001"],
                "field_name": [included_field, "permission_collab", excluded_field],
                "value": [
                    "Test",
                    Values.PID247.permission_collab[
                        "NO, you may not share my child's records."
                    ],
                    Values.PID247.intake_ready["Ready to Send to Intake Redcap"],
                ],
                "redcap_repeat_instrument": ["", "", ""],
                "redcap_repeat_instance": ["", "", ""],
            }
        )

        # Only 2 Fields should pass the filter
        expected_filtered_rows = 2

        with patch_redcap_transfer_module(
            fetch_return=data.copy(),
            push_return=expected_filtered_rows,
            update_return=expected_filtered_rows,
        ) as mocks:
            # Execute
            transfer_module.main()

            # Assert
            call_df = mocks["push"].call_args[1]["df"]
            renamed_field = Fields.rename_247_to_744[included_field]

            # After rename, should have first_name and permission_collab
            assert renamed_field in call_df["field_name"].values
            assert "permission_collab" in call_df["field_name"].values
            # Should not include "intake_ready" (not in import_744 list)
            assert excluded_field not in call_df["field_name"].values

    def test_main_decrements_permission_collab(self):
        """Test that permission_collab values are decremented by 1."""
        original_permission = Values.PID744.permission_collab["No"]
        expected_permission = str(int(original_permission) - 1)

        data = pd.DataFrame(
            {
                "record": ["001", "001"],
                "field_name": ["permission_collab", "consent1"],
                "value": [original_permission, "Test"],
                "redcap_repeat_instrument": ["", ""],
                "redcap_repeat_instance": ["", ""],
            }
        )

        expected_rows = calculate_total_eav_rows(data)

        with patch_redcap_transfer_module(
            fetch_return=data,
            push_return=expected_rows,
            update_return=expected_rows,
        ) as mocks:
            # Execute
            transfer_module.main()

            # Assert
            call_df = mocks["push"].call_args[1]["df"]
            assert_permission_decremented(
                call_df, original_permission, expected_permission
            )

    def test_main_handles_repeating_instances(self):
        """Test that most recent repeating instance is kept."""
        old_value = "Old Name"
        new_value = "Alec Holland"

        data = pd.DataFrame(
            {
                "record": ["001", "001", "001", "001"],
                "field_name": [
                    "consent1",
                    "consent1",
                    "permission_collab",
                    "permission_collab",
                ],
                "value": [old_value, new_value, "2", "3"],
                "redcap_repeat_instrument": ["form1", "form1", "form2", "form2"],
                "redcap_repeat_instance": [1, 2, 1, 2],
            }
        )

        # After deduplication, should have 2 Fields for 1 record
        expected_rows_after_dedup = 2

        with patch_redcap_transfer_module(
            fetch_return=data,
            push_return=expected_rows_after_dedup,
            update_return=expected_rows_after_dedup,
        ) as mocks:
            # Execute
            transfer_module.main()

            # Assert
            call_df = mocks["push"].call_args[1]["df"]
            renamed_field = Fields.rename_247_to_744["consent1"]
            name_row = call_df[call_df["field_name"] == renamed_field]

            assert len(name_row) > 0, f"No {renamed_field} found"
            assert name_row["value"].iloc[0] == new_value
            assert "redcap_repeat_instance" not in call_df.columns

    @patch("hbnmigration.from_redcap.to_redcap.redcap_variables")
    def test_main_uses_correct_tokens(self, mock_redcap_vars):
        """Test that correct tokens are used for each project."""
        token_247 = "token_247"
        token_744 = "token_744"
        mock_redcap_vars.Tokens.pid247 = token_247
        mock_redcap_vars.Tokens.pid744 = token_744

        data = pd.DataFrame(
            {
                "record": ["001"],
                "field_name": ["consent1"],
                "value": ["Test"],
                "redcap_repeat_instrument": [""],
                "redcap_repeat_instance": [""],
            }
        )

        expected_rows = calculate_total_eav_rows(data)

        with patch_redcap_transfer_module(
            fetch_return=data,
            push_return=expected_rows,
            update_return=expected_rows,
        ) as mocks:
            # Execute
            transfer_module.main()

            # Assert - first push should use PID 744 token
            assert mocks["push"].call_args[1]["token"] == token_744

    def test_main_removes_duplicate_Fields(self) -> None:  # noqa: N802
        """Test that duplicate field values are properly deduplicated."""
        old_value = "Old Value"
        new_value = "New Value"

        data = pd.DataFrame(
            {
                "record": ["001", "001", "001"],
                "field_name": ["consent1", "consent1", "permission_collab"],
                "value": [old_value, new_value, "3"],
                "redcap_repeat_instrument": ["", "", ""],
                "redcap_repeat_instance": [1, 2, 1],
            }
        )

        # After deduplication, should have 2 unique Fields
        expected_unique_Fields = len(data["field_name"].unique())  # noqa N806

        with patch_redcap_transfer_module(
            fetch_return=data,
            push_return=expected_unique_Fields,
            update_return=expected_unique_Fields,
        ) as mocks:
            # Execute
            transfer_module.main()

            # Assert
            call_df = mocks["push"].call_args[1]["df"]
            renamed_field = Fields.rename_247_to_744["consent1"]
            name_entries = call_df[call_df["field_name"] == renamed_field]
            assert len(name_entries) == 1


# ============================================================================
# Integration Tests
# ============================================================================


class TestIntegration:
    """Integration tests for the complete workflow."""

    @patch("hbnmigration.from_redcap.to_redcap.redcap_api_push")
    @patch("hbnmigration.from_redcap.to_redcap.fetch_api_data")
    def test_full_workflow_two_records(self, mock_fetch_api, mock_push):
        """Test complete workflow with two records."""
        EXPECTED_API_CALLS = 2  # noqa: N806
        """One to destination, one to update source."""
        source_data = pd.DataFrame(
            {
                "record": ["001", "001", "002", "002"],
                "field_name": [
                    "consent1",
                    "permission_collab",
                    "consent1",
                    "permission_collab",
                ],
                "value": [
                    "Alec Holland",
                    Values.PID247.permission_collab[
                        "NO, you may not share my child's records."
                    ],
                    "Abby Arcane",
                    Values.PID247.permission_collab[
                        "YES, you may share my child's records."
                    ],
                ],
                "redcap_repeat_instrument": ["", "", "", ""],
                "redcap_repeat_instance": ["", "", "", ""],
            }
        )

        num_records = count_records_in_eav(source_data)
        total_rows = calculate_total_eav_rows(source_data)

        mock_fetch_api.return_value = source_data
        mock_push.return_value = num_records

        # Execute
        transfer_module.main()

        # Assert
        assert mock_push.call_count == EXPECTED_API_CALLS

        # First call: Push to PID 744
        first_call_df = mock_push.call_args_list[0][1]["df"]
        assert len(first_call_df) == total_rows

        # Second call: Update source PID 247
        second_call_df = mock_push.call_args_list[1][1]["df"]
        assert len(second_call_df) == num_records
        assert all(
            second_call_df["value"]
            == Values.PID247.intake_ready[
                "Participant information already sent to HBN - Intake Redcap project"
            ]
        )

    @patch("hbnmigration.from_redcap.to_redcap.redcap_api_push")
    @patch("hbnmigration.from_redcap.to_redcap.fetch_api_data")
    def test_full_workflow_parliament_of_trees(self, mock_fetch_api, mock_push):
        """Test complete workflow with Parliament of Trees members."""
        source_data = pd.DataFrame(
            {
                "record": ["ST001", "ST001", "AA001", "AA001", "TE001", "TE001"],
                "field_name": [
                    "consent1",
                    "permission_collab",
                    "consent1",
                    "permission_collab",
                    "consent1",
                    "permission_collab",
                ],
                "value": [
                    "Alec Holland",
                    Values.PID247.permission_collab[
                        "NO, you may not share my child's records."
                    ],
                    "Abby Arcane",
                    Values.PID247.permission_collab[
                        "YES, you may share my child's records."
                    ],
                    "Tefé Holland",
                    Values.PID247.permission_collab[
                        "NO, you may not share my child's records."
                    ],
                ],
                "redcap_repeat_instrument": ["", "", "", "", "", ""],
                "redcap_repeat_instance": ["", "", "", "", "", ""],
            }
        )

        num_records = count_records_in_eav(source_data)
        mock_fetch_api.return_value = source_data
        mock_push.return_value = num_records

        # Execute
        transfer_module.main()

        # Assert
        first_call_df = mock_push.call_args_list[0][1]["df"]

        # Verify all three Parliament members are present
        assert len(first_call_df["record"].unique()) == num_records
        assert "ST001" in first_call_df["record"].values
        assert "AA001" in first_call_df["record"].values
        assert "TE001" in first_call_df["record"].values

        # Verify permission_collab values are decremented (2→1, 1→0)
        expected_permissions = ["0", "1"]  # Sorted order
        actual_permissions = get_unique_field_values(first_call_df, "permission_collab")
        assert actual_permissions == expected_permissions


# ============================================================================
# Edge Case Tests
# ============================================================================


class TestEdgeCases:
    """Tests for edge cases and error conditions."""

    def test_handles_non_numeric_permission_collab(self):
        """Test handling of non-numeric permission_collab values."""
        data = pd.DataFrame(
            {
                "record": ["001", "001"],
                "field_name": ["permission_collab", "consent1"],
                "value": ["invalid", "Test"],
                "redcap_repeat_instrument": ["", ""],
                "redcap_repeat_instance": ["", ""],
            }
        )

        expected_rows = calculate_total_eav_rows(data)

        with patch_redcap_transfer_module(
            fetch_return=data,
            push_return=expected_rows,
            update_return=expected_rows,
        ) as mocks:
            transfer_module.main()

            call_df = mocks["push"].call_args_list[0][1]["df"]
            perm_row = call_df[call_df["field_name"] == "permission_collab"]
            assert len(perm_row) > 0
            # Non-numeric values become NaN after pd.to_numeric with errors='coerce'
            assert pd.isna(perm_row["value"].iloc[0])

    def test_handles_missing_required_columns(self):
        """Test handling of data missing required columns."""
        bad_data = pd.DataFrame(
            {
                "record": ["001"],
                "value": ["test"],
            }
        )

        with patch_redcap_transfer_module(fetch_return=bad_data):
            with pytest.raises((KeyError, AttributeError)):
                transfer_module.main()

    def test_handles_empty_string_permission_collab(self):
        """Test handling of empty string in permission_collab."""
        data = pd.DataFrame(
            {
                "record": ["001", "001"],
                "field_name": ["permission_collab", "consent1"],
                "value": ["", "Test"],
                "redcap_repeat_instrument": ["", ""],
                "redcap_repeat_instance": ["", ""],
            }
        )

        expected_rows = calculate_total_eav_rows(data)

        with patch_redcap_transfer_module(
            fetch_return=data,
            push_return=expected_rows,
            update_return=expected_rows,
        ) as mocks:
            transfer_module.main()

            call_df = mocks["push"].call_args_list[0][1]["df"]
            perm_row = call_df[call_df["field_name"] == "permission_collab"]
            assert len(perm_row) > 0
            # Empty strings become NaN after pd.to_numeric with errors='coerce'
            assert pd.isna(perm_row["value"].iloc[0])

    def test_handles_zero_permission_collab(self):
        """Test that zero permission_collab becomes -1."""
        original_value = "0"
        expected_value = str(int(original_value) - 1)

        data = pd.DataFrame(
            {
                "record": ["001", "001"],
                "field_name": ["permission_collab", "consent1"],
                "value": [original_value, "Test"],
                "redcap_repeat_instrument": ["", ""],
                "redcap_repeat_instance": ["", ""],
            }
        )

        expected_rows = calculate_total_eav_rows(data)

        with patch_redcap_transfer_module(
            fetch_return=data,
            push_return=expected_rows,
            update_return=expected_rows,
        ) as mocks:
            transfer_module.main()

            call_df = mocks["push"].call_args_list[0][1]["df"]
            assert_permission_decremented(call_df, original_value, expected_value)

    def test_handles_single_record(self):
        """Test workflow with only one record."""
        record_id = "001"
        data = pd.DataFrame(
            {
                "record": [record_id],
                "field_name": ["consent1"],
                "value": ["Solo Swamp Thing"],
                "redcap_repeat_instrument": [""],
                "redcap_repeat_instance": [""],
            }
        )

        expected_rows = calculate_total_eav_rows(data)

        with patch_redcap_transfer_module(
            fetch_return=data,
            push_return=expected_rows,
            update_return=expected_rows,
        ) as mocks:
            transfer_module.main()

            first_call_df = mocks["push"].call_args_list[0][1]["df"]
            assert len(first_call_df) == expected_rows
            assert first_call_df["record"].iloc[0] == record_id
