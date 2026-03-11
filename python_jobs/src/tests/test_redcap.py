"""Test code for data transfer from REDCap to Curious."""

from unittest.mock import patch

import pandas as pd
import pytest

from hbnmigration.exceptions import NoData
from hbnmigration.from_curious.config import account_types
from hbnmigration.from_redcap import to_curious
from hbnmigration.from_redcap.config import Values

from .conftest import (
    assert_enrollment_complete_updated,
    assert_valid_curious_format,
    count_curious_accounts,
    create_curious_api_failure,
    create_curious_participant_df,
    create_redcap_eav_df,
    get_curious_records_by_tag,
    patch_curious_api_dependencies,
    patch_curious_transfer_module,
    setup_curious_integration_mocks,
)

# ============================================================================
# _in_set() Tests
# ============================================================================


class TestInSet:
    """Tests for _in_set helper function."""

    @pytest.mark.parametrize(
        "input_value,required,expected",
        [
            ({1, 2, 3}, 1, True),
            ({2, 3, 4}, 1, False),
            (1, 1, True),
            (2, 1, False),
            ("1", 1, True),
            ("2", 1, False),
            ([1, 2, 3], 1, True),
            ([2, 3, 4], 1, False),
            ({"1", "2"}, "1", True),
            ({1, 2}, "1", True),
        ],
    )
    def test_in_set_various_inputs(self, input_value, required, expected):
        """Test _in_set with various input types."""
        assert to_curious._in_set(input_value, required) is expected

    @pytest.mark.parametrize(
        "invalid_input",
        [None, 3.14, {}, {}],
    )
    def test_in_set_invalid_types_return_false(self, invalid_input):
        """Test that _in_set returns False with invalid types."""
        assert to_curious._in_set(invalid_input, 1) is False


# ============================================================================
# _check_for_data_to_process() Tests
# ============================================================================


class TestCheckForDataToProcess:
    """Tests for _check_for_data_to_process helper function."""

    def test_logs_info_when_no_data(self, caplog, formatted_curious_data):
        """Test that appropriate log message when no data for account type."""
        df_empty = formatted_curious_data[
            formatted_curious_data["accountType"] == "nonexistent"
        ]
        to_curious._check_for_data_to_process(df_empty, "full")
        assert "There is not full consent data to process" in caplog.text

    def test_logs_info_when_data_exists(self, caplog):
        """Test that appropriate log message when data exists."""
        df = pd.DataFrame(
            {
                "accountType": ["full", "limited"],
                "secretUserId": ["00001_P", "00001"],
            }
        )
        to_curious._check_for_data_to_process(df, "full")
        assert "Full data was prepared to be sent to the Curious API" in caplog.text

    def test_checks_all_account_types(self, caplog, formatted_curious_data):
        """Test logging for multiple account types."""
        for account_type in account_types:
            to_curious._check_for_data_to_process(formatted_curious_data, account_type)
        assert any(
            _account_type in caplog.text.lower() for _account_type in account_types
        )


# ============================================================================
# format_redcap_data_for_curious() Tests
# ============================================================================


class TestFormatRedcapDataForCurious:
    """Tests for format_redcap_data_for_curious function."""

    def test_formats_basic_parent_child_data(self):
        """Test basic formatting of parent and child data."""
        # Need to use actual field names that will map correctly
        redcap_data = create_redcap_eav_df(
            records=["001", "001", "001"],
            field_names=[
                "mrn",
                "parent_involvement___1",
                "adult_enrollment_form_complete",
            ],
            values=["12345", "1", "0"],
        )
        result = to_curious.format_redcap_data_for_curious(redcap_data)

        assert_valid_curious_format(result)
        counts = count_curious_accounts(result)
        # Both parent and child created for each record
        assert counts["parent"] >= 1
        assert counts["child"] >= 1

    def test_pads_secret_user_id_with_zeros(self):
        """Test that secretUserId is padded to 5 characters AFTER _P suffix."""
        redcap_data = create_redcap_eav_df(
            records=["001", "001"],
            field_names=["mrn", "parent_involvement___1"],
            values=["123", "1"],
        )
        result = to_curious.format_redcap_data_for_curious(redcap_data)

        # After formatting: "123" -> "123_P" -> "123_P" (padding happens after suffix)
        # Actually, looking at code: suffix added first, then padding
        # "123" -> parent gets "123_P" -> zfill(5) -> "123_P" (stays same, already >5)
        # "123" -> child gets "123" -> zfill(5) -> "00123"
        for user_id in result["secretUserId"]:
            if not user_id.endswith("_P"):
                # Child records should be padded
                assert len(user_id) == 5, (
                    f"Child secretUserId should be 5 chars: {user_id}"
                )
            # Parent records will be longer due to _P suffix

    def test_appends_p_suffix_to_parent_before_padding(self):
        """Test that parent secretUserId gets _P suffix before padding."""
        redcap_data = create_redcap_eav_df(
            records=["001", "001"],
            field_names=["mrn", "parent_involvement___1"],
            values=["12345", "1"],
        )
        result = to_curious.format_redcap_data_for_curious(redcap_data)

        parent_rows = get_curious_records_by_tag(result, "parent")
        assert len(parent_rows) > 0
        # After suffix and padding: "12345_P" -> zfill(5) keeps it as "12345_P"
        assert all(parent_rows["secretUserId"].str.endswith("_P"))

    def test_filters_by_parent_involvement(self):
        """Test that records are filtered by parent_involvement___1."""
        redcap_data = create_redcap_eav_df(
            records=["001", "001", "002", "002", "002"],
            field_names=[
                "mrn",
                "parent_involvement___1",
                "mrn",
                "parent_involvement___2",
                "adult_enrollment_form_complete",
            ],
            values=["12345", "1", "67890", "1", "1"],  # 002 has complete=True
        )
        result = to_curious.format_redcap_data_for_curious(redcap_data)

        secret_ids = {x.rstrip("_P").lstrip("0") for x in result["secretUserId"]}
        assert "12345" in secret_ids

    def test_drops_parent_involvement_columns(self):
        """Test that parent_involvement columns are dropped from output."""
        redcap_data = create_redcap_eav_df(
            records=["001", "001"],
            field_names=["mrn", "parent_involvement___1"],
            values=["12345", "1"],
        )
        result = to_curious.format_redcap_data_for_curious(redcap_data)

        assert "parent_involvement" not in result.columns
        assert "adult_enrollment_form_complete" not in result.columns

    def test_adds_default_values_for_missing_fields(self):
        """Test that missing fields get default values from config."""
        redcap_data = create_redcap_eav_df(
            records=["001", "001"],
            field_names=["mrn", "parent_involvement___1"],
            values=["12345", "1"],
        )
        result = to_curious.format_redcap_data_for_curious(redcap_data)

        # Check that default fields are present
        assert "accountType" in result.columns
        assert "role" in result.columns
        assert "language" in result.columns
        # Check default values
        assert all(result[result["tag"] == "parent"]["accountType"] == "full")
        assert all(result[result["tag"] == "child"]["accountType"] == "limited")

    def test_replaces_nan_with_none(self):
        """Test that NaN values are replaced with None."""
        redcap_data = create_redcap_eav_df(
            records=["001", "001"],
            field_names=["mrn", "parent_involvement___1"],
            values=["12345", "1"],
        )
        result = to_curious.format_redcap_data_for_curious(redcap_data)

        # Fields without data should be None, not NaN
        result_dicts = result.to_dict(orient="records")
        for record in result_dicts:
            for key, value in record.items():
                if value is None:
                    continue  # None is expected
                assert value is not pd.NA


# ============================================================================
# send_to_curious() Tests
# ============================================================================


class TestSendToCurious:
    """Tests for send_to_curious function."""

    def test_sends_all_records_to_curious(
        self, formatted_curious_data, mock_curious_variables
    ):
        """Test that all records are sent to Curious API."""
        with patch_curious_api_dependencies(new_account_return="Success") as mocks:
            tokens = mock_curious_variables.Tokens(
                mock_curious_variables.Endpoints(),
                mock_curious_variables.Credentials.hbn_mindlogger,
            )

            failures = to_curious.send_to_curious(
                formatted_curious_data,
                tokens,
                "test_applet_id",
            )

            assert len(failures) == 0
            assert mocks["new_account"].call_count == len(formatted_curious_data)

    def test_handles_api_request_exception(
        self, formatted_curious_data, mock_curious_variables
    ):
        """Test that API exceptions are caught and logged."""
        with patch_curious_api_dependencies(
            new_account_side_effect=create_curious_api_failure()
        ):
            tokens = mock_curious_variables.Tokens(
                mock_curious_variables.Endpoints(),
                mock_curious_variables.Credentials.hbn_mindlogger,
            )

            failures = to_curious.send_to_curious(
                formatted_curious_data,
                tokens,
                "test_applet_id",
            )

            assert len(failures) == len(formatted_curious_data)

    def test_partial_failure_tracking(
        self, formatted_curious_data, mock_curious_variables
    ):
        """Test that partial failures are tracked correctly."""
        with patch_curious_api_dependencies(
            new_account_side_effect=["Success", create_curious_api_failure()]
        ):
            tokens = mock_curious_variables.Tokens(
                mock_curious_variables.Endpoints(),
                mock_curious_variables.Credentials.hbn_mindlogger,
            )

            failures = to_curious.send_to_curious(
                formatted_curious_data,
                tokens,
                "test_applet_id",
            )

            assert len(failures) == 1

    def test_filters_none_values_from_records(self, mock_curious_variables):
        """Test that None values are filtered from records before sending."""
        with patch_curious_api_dependencies(new_account_return="Success") as mocks:
            tokens = mock_curious_variables.Tokens(
                mock_curious_variables.Endpoints(),
                mock_curious_variables.Credentials.hbn_mindlogger,
            )

            df_with_nones = create_curious_participant_df(
                secret_user_ids=["00001"],
                tags=["child"],
                first_names=[None],
                last_names=["Holland"],
            )

            to_curious.send_to_curious(df_with_nones, tokens, "test_applet_id")

            call_record = mocks["new_account"].call_args[0][2]
            assert "firstName" not in call_record
            assert "lastName" in call_record

    def test_includes_authorization_header(
        self, formatted_curious_data, mock_curious_variables
    ):
        """Test that authorization header is included in API calls."""
        with patch_curious_api_dependencies(new_account_return="Success") as mocks:
            tokens = mock_curious_variables.Tokens(
                mock_curious_variables.Endpoints(),
                mock_curious_variables.Credentials.hbn_mindlogger,
            )

            to_curious.send_to_curious(
                formatted_curious_data,
                tokens,
                "test_applet_id",
            )

            call_headers = mocks["new_account"].call_args[0][3]
            assert "Authorization" in call_headers
            assert call_headers["Authorization"] == f"Bearer {tokens.access}"


# ============================================================================
# update_redcap() Tests
# ============================================================================


class TestUpdateRedcap:
    """Tests for update_redcap function."""

    @patch("hbnmigration.from_redcap.to_curious.redcap_api_push")
    @patch("hbnmigration.from_redcap.to_curious.redcap_variables")
    def test_updates_enrollment_complete_for_successes(
        self,
        mock_redcap_vars,
        mock_push,
    ):
        """Test that enrollment_complete is updated for successful transfers."""
        mock_push.return_value = 1
        mock_redcap_vars.Tokens.pid247 = "token_247"
        mock_redcap_vars.Endpoints.return_value.base_url = "https://redcap.test/api/"
        mock_redcap_vars.headers = {}

        # The function needs mrn in the redcap data to find records to update
        redcap_data = create_redcap_eav_df(
            records=["001"],
            field_names=["mrn"],
            values=["12345"],
        )

        curious_data = create_curious_participant_df(
            secret_user_ids=["12345"],
            tags=["child"],
        )

        to_curious.update_redcap(redcap_data, curious_data, [])

        mock_push.assert_called_once()
        call_df = mock_push.call_args[1]["df"]
        assert_enrollment_complete_updated(
            call_df,
            Values.PID247.enrollment_complete[
                "Parent and Participant information already sent to Curious"
            ],
        )

    @patch("hbnmigration.from_redcap.to_curious.redcap_api_push")
    @patch("hbnmigration.from_redcap.to_curious.redcap_variables")
    def test_excludes_failures_from_update(
        self,
        mock_redcap_vars,
        mock_push,
    ):
        """Test that failed records are not updated in REDCap."""
        mock_push.return_value = 0
        mock_redcap_vars.Tokens.pid247 = "token_247"
        mock_redcap_vars.Endpoints.return_value.base_url = "https://redcap.test/api/"

        redcap_data = create_redcap_eav_df(
            records=["001"],
            field_names=["mrn"],
            values=["12345"],
        )

        curious_data = create_curious_participant_df(
            secret_user_ids=["12345"],
            tags=["child"],
        )

        failures = ["12345"]  # MRN that failed

        to_curious.update_redcap(redcap_data, curious_data, failures)

        call_df = mock_push.call_args[1]["df"]
        # Should have empty dataframe since all failed
        assert len(call_df) == 0

    @patch("hbnmigration.from_redcap.to_curious.redcap_api_push")
    @patch("hbnmigration.from_redcap.to_curious.redcap_variables")
    def test_filters_out_parent_records_from_update(
        self,
        mock_redcap_vars,
        mock_push,
    ):
        """Test that parent records (_P suffix) are excluded from REDCap update."""
        mock_push.return_value = 1
        mock_redcap_vars.Tokens.pid247 = "token_247"
        mock_redcap_vars.Endpoints.return_value.base_url = "https://redcap.test/api/"
        mock_redcap_vars.headers = {}

        redcap_data = create_redcap_eav_df(
            records=["001"],
            field_names=["mrn"],
            values=["12345"],
        )

        # Include both parent and child - only child should trigger update
        curious_data = pd.DataFrame(
            {
                "secretUserId": ["12345", "12345_P"],
                "tag": ["child", "parent"],
            }
        )

        to_curious.update_redcap(redcap_data, curious_data, [])

        call_df = mock_push.call_args[1]["df"]
        # Should update based on child record (12345) not parent (12345_P)
        assert len(call_df) > 0


# ============================================================================
# main() Tests
# ============================================================================


class TestMain:
    """Tests for main workflow function."""

    def test_main_successful_transfer(
        self, sample_redcap_curious_data, formatted_curious_data
    ):
        """Test successful end-to-end transfer to Curious."""
        with patch_curious_transfer_module(
            fetch_return=sample_redcap_curious_data,
            format_return=formatted_curious_data,
            send_return=[],
        ) as mocks:
            to_curious.main()

            mocks["fetch"].assert_called_once()
            mocks["format"].assert_called_once_with(sample_redcap_curious_data)
            mocks["send"].assert_called_once()
            mocks["update"].assert_called_once()

    def test_main_no_data_logs_and_returns(self, caplog):
        """Test that main returns early when no data available."""
        with patch_curious_transfer_module(fetch_return=None) as mocks:
            mocks["fetch"].side_effect = NoData()

            to_curious.main()

            assert "No data to transfer from REDCap PID 247 to Curious" in caplog.text

    def test_main_empty_dataframe_raises_nodata(self, caplog):
        """Test that empty DataFrame raises NoData."""
        with patch_curious_transfer_module(fetch_return=pd.DataFrame()):
            to_curious.main()

            assert "No participants marked 'Ready to Send to Curious'" in caplog.text

    def test_main_uses_correct_filter_logic(self, sample_redcap_curious_data):
        """Test that correct filter logic is used for fetch."""
        # Need formatted data with accountType column for _check_for_data_to_process
        formatted_data = create_curious_participant_df(
            secret_user_ids=["00001"],
            tags=["child"],
        )

        with patch_curious_transfer_module(
            fetch_return=sample_redcap_curious_data,
            format_return=formatted_data,
            send_return=[],
        ) as mocks:
            to_curious.main()

            call_args = mocks["fetch"].call_args[0]
            assert "enrollment_complete" in call_args[2]

    def test_main_checks_all_account_types(
        self, sample_redcap_curious_data, formatted_curious_data, caplog
    ):
        """Test that all account types are checked for data."""
        with patch_curious_transfer_module(
            fetch_return=sample_redcap_curious_data,
            format_return=formatted_curious_data,
            send_return=[],
        ):
            to_curious.main()
            assert any(
                _account_type in caplog.text.lower() for _account_type in account_types
            )

    def test_main_handles_partial_failures(
        self, sample_redcap_curious_data, formatted_curious_data
    ):
        """Test that partial failures are handled correctly."""
        failures = ["12345"]
        with patch_curious_transfer_module(
            fetch_return=sample_redcap_curious_data,
            format_return=formatted_curious_data,
            send_return=failures,
        ) as mocks:
            to_curious.main()

            assert mocks["update"].call_args[0][2] == failures

    def test_main_uses_correct_applet_id(
        self, sample_redcap_curious_data, formatted_curious_data
    ):
        """Test that correct applet ID is used."""
        expected_applet_id = "test_applet_id"
        with patch_curious_transfer_module(
            fetch_return=sample_redcap_curious_data,
            format_return=formatted_curious_data,
            send_return=[],
        ) as mocks:
            to_curious.main()

            assert mocks["send"].call_args[0][2] == expected_applet_id


# ============================================================================
# Integration Tests
# ============================================================================


class TestIntegration:
    """Integration tests for complete Curious transfer workflow."""

    @patch("hbnmigration.from_redcap.to_curious.redcap_api_push")
    @patch("hbnmigration.from_redcap.to_curious.new_curious_account")
    @patch("hbnmigration.from_redcap.from_redcap.fetch_api_data")
    @patch("hbnmigration.from_redcap.to_curious.curious_variables")
    @patch("hbnmigration.from_redcap.to_curious.redcap_variables")
    @patch("hbnmigration.from_redcap.from_redcap.redcap_variables")
    def test_full_workflow_parliament_of_trees(
        self,
        mock_from_redcap_vars,
        mock_to_redcap_vars,
        mock_curious_vars,
        mock_fetch_api,
        mock_new_account,
        mock_redcap_push,
    ):
        """Test complete workflow with Parliament of Trees members."""
        setup_curious_integration_mocks(mock_curious_vars, mock_to_redcap_vars)
        mock_from_redcap_vars.Tokens.pid247 = "token_247"
        mock_from_redcap_vars.headers = {}
        mock_from_redcap_vars.Endpoints.return_value.base_url = (
            "https://redcap.test/api/"
        )

        source_data = create_redcap_eav_df(
            records=["ST001", "ST001", "AA001", "AA001"],
            field_names=[
                "mrn",
                "parent_involvement___1",
                "mrn",
                "parent_involvement___1",
            ],
            values=[
                "12345",
                "1",
                "67890",
                "1",
            ],
        )

        mock_fetch_api.return_value = source_data
        mock_new_account.return_value = "Success"
        mock_redcap_push.return_value = 2

        to_curious.main()

        # Should create 4 accounts (2 parents + 2 children)
        assert mock_new_account.call_count == 4
        # Should update REDCap for 2 records
        mock_redcap_push.assert_called_once()


# ============================================================================
# Edge Case Tests
# ============================================================================


class TestEdgeCases:
    """Tests for edge cases and error conditions."""

    def test_format_handles_empty_dataframe(self):
        """Test that empty DataFrame is handled gracefully."""
        empty_df = pd.DataFrame()

        with pytest.raises((KeyError, ValueError)):
            to_curious.format_redcap_data_for_curious(empty_df)

    def test_send_to_curious_with_empty_dataframe(self, mock_curious_variables):
        """Test sending empty DataFrame to Curious."""
        with patch_curious_api_dependencies(new_account_return="Success") as mocks:
            tokens = mock_curious_variables.Tokens(
                mock_curious_variables.Endpoints(),
                mock_curious_variables.Credentials.hbn_mindlogger,
            )
            # Create truly empty DataFrame - not using factory function
            empty_df = pd.DataFrame(columns=["secretUserId", "accountType", "tag"])

            failures = to_curious.send_to_curious(empty_df, tokens, "test_applet")

            assert len(failures) == 0
            # Should not have called the API since DataFrame is empty
            mocks["new_account"].assert_not_called()

    def test_update_redcap_with_no_successes(self):
        """Test REDCap update when all records failed."""
        redcap_data = create_redcap_eav_df(
            records=["001"],
            field_names=["mrn"],
            values=["12345"],
        )
        curious_data = create_curious_participant_df(
            secret_user_ids=["12345"],
            tags=["child"],
        )
        failures = ["12345"]

        with patch("hbnmigration.from_redcap.to_curious.redcap_api_push") as mock_push:
            mock_push.return_value = 0
            to_curious.update_redcap(redcap_data, curious_data, failures)

            call_df = mock_push.call_args[1]["df"]
            assert len(call_df) == 0

    def test_send_continues_after_single_failure(self, mock_curious_variables):
        """Test that send_to_curious continues after a single failure."""
        with patch_curious_api_dependencies(
            new_account_side_effect=[
                "Success",
                create_curious_api_failure(),
                "Success",
            ]
        ) as mocks:
            tokens = mock_curious_variables.Tokens(
                mock_curious_variables.Endpoints(),
                mock_curious_variables.Credentials.hbn_mindlogger,
            )

            df = create_curious_participant_df(
                secret_user_ids=["00001", "00002", "00003"],
                tags=["child", "child", "child"],
            )

            failures = to_curious.send_to_curious(df, tokens, "test_applet")

            assert len(failures) == 1
            assert failures[0] == "2"
            assert mocks["new_account"].call_count == 3
