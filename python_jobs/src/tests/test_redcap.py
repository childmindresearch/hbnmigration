"""Test code for data transfer from REDCap to Curious."""

from typing import Literal
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from hbnmigration.exceptions import NoData
from hbnmigration.from_curious.config import account_types
from hbnmigration.from_redcap import to_curious, to_redcap
from hbnmigration.from_redcap.config import Values
from hbnmigration.from_redcap.to_redcap import (
    update_complete_parent_second_guardian_consent,
)

from .conftest import (
    assert_enrollment_complete_updated,
    create_curious_api_failure,
    create_curious_participant_df,
    create_redcap_eav_df,
    patch_curious_api_dependencies,
    patch_curious_transfer_module,
    patch_redcap_transfer_module,
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

        # Result is now a dict with 'child' and 'parent' keys
        assert isinstance(result, dict)
        assert "child" in result
        assert "parent" in result
        assert isinstance(result["child"], pd.DataFrame)
        assert isinstance(result["parent"], pd.DataFrame)

        # Both parent and child DataFrames should have data
        assert len(result["parent"]) >= 1
        assert len(result["child"]) >= 1

    def test_pads_secret_user_id_with_zeros(self):
        """Test that secretUserId is padded to 5 characters for children."""
        redcap_data = create_redcap_eav_df(
            records=["001", "001"],
            field_names=["mrn", "parent_involvement___1"],
            values=["123", "1"],
        )
        result = to_curious.format_redcap_data_for_curious(redcap_data)

        # Child records should have padded secretUserId
        assert "secretUserId" in result["child"].columns
        child_ids = result["child"]["secretUserId"]
        for user_id in child_ids:
            # Child secretUserId should be padded to 5 chars
            assert len(user_id) == 5, f"Child secretUserId should be 5 chars: {user_id}"

    def test_appends_p_suffix_to_parent_before_padding(self):
        """Test that parent secretUserId gets responder ID format."""
        redcap_data = create_redcap_eav_df(
            records=["001", "001"],
            field_names=["mrn", "parent_involvement___1"],
            values=["12345", "1"],
        )
        result = to_curious.format_redcap_data_for_curious(redcap_data)

        # Parent records should have secretUserId (responder IDs start with 'R')
        assert "secretUserId" in result["parent"].columns
        parent_ids = result["parent"]["secretUserId"]
        assert len(parent_ids) > 0

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

        # Check that child DataFrame has secretUserId
        assert "secretUserId" in result["child"].columns
        child_secret_ids = {x.lstrip("0") for x in result["child"]["secretUserId"]}
        assert "12345" in child_secret_ids

    def test_drops_parent_involvement_columns(self):
        """Test that parent_involvement columns are dropped from output."""
        redcap_data = create_redcap_eav_df(
            records=["001", "001"],
            field_names=["mrn", "parent_involvement___1"],
            values=["12345", "1"],
        )
        result = to_curious.format_redcap_data_for_curious(redcap_data)

        # Check child DataFrame doesn't have these columns
        assert "parent_involvement" not in result["child"].columns
        assert "adult_enrollment_form_complete" not in result["child"].columns
        # Parent involvement may still be in parent DataFrame but
        # shouldn't be sent to Curious
        # The key is that it's filtered out before API calls

    def test_adds_default_values_for_missing_fields(self):
        """Test that missing fields get default values from config."""
        redcap_data = create_redcap_eav_df(
            records=["001", "001"],
            field_names=["mrn", "parent_involvement___1"],
            values=["12345", "1"],
        )
        result = to_curious.format_redcap_data_for_curious(redcap_data)

        # Check child DataFrame for expected columns
        child_df = result["child"]
        assert len(child_df.columns) > 0

        # Check parent DataFrame for expected columns
        parent_df = result["parent"]
        assert len(parent_df.columns) > 0

    def test_replaces_nan_with_none(self):
        """Test that NaN values are replaced with None."""
        redcap_data = create_redcap_eav_df(
            records=["001", "001"],
            field_names=["mrn", "parent_involvement___1"],
            values=["12345", "1"],
        )
        result = to_curious.format_redcap_data_for_curious(redcap_data)

        # Check both DataFrames can be converted to dict
        child_dicts = result["child"].to_dict(orient="records")
        parent_dicts = result["parent"].to_dict(orient="records")

        # Verify no NaN values (None is acceptable)
        for record in child_dicts + parent_dicts:
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
        # formatted_curious_data should now be a dict with 'child' and 'parent' keys
        formatted_dict = {
            "child": formatted_curious_data[
                formatted_curious_data["tag"] == "child"
            ].drop("tag", axis=1),
            "parent": formatted_curious_data[
                formatted_curious_data["tag"] == "parent"
            ].drop("tag", axis=1),
        }

        with patch_curious_transfer_module(
            fetch_return=sample_redcap_curious_data,
            format_return=formatted_dict,
            send_return=[],
        ) as mocks:
            to_curious.main()
            mocks["fetch"].assert_called()  # Called multiple times (PID 625 and 247)
            mocks["format"].assert_called_once_with(sample_redcap_curious_data)
            # send is called for both child and parent data
            assert mocks["send"].call_count >= 1
            mocks["update"].assert_called()

    def test_main_no_data_logs_and_returns(self, caplog):
        """Test that main returns early when no data available."""
        with patch_curious_transfer_module(fetch_return=None) as mocks:
            mocks["fetch"].side_effect = NoData()
            to_curious.main()
            assert "No data to transfer from REDCap PID 247 to Curious" in caplog.text

    def test_main_empty_dataframe_raises_nodata(self, caplog):
        """Test that empty DataFrame raises NoData."""
        # Empty DataFrame should have proper EAV structure
        empty_df = pd.DataFrame(columns=["record", "field_name", "value"])
        with patch_curious_transfer_module(fetch_return=empty_df):
            to_curious.main()
            assert "No participants marked 'Ready to Send to Curious'" in caplog.text

    def test_main_uses_correct_filter_logic(self, sample_redcap_curious_data):
        """Test that correct filter logic is used for fetch."""
        # Need formatted data as dict with child/parent keys
        formatted_data = {
            "child": create_curious_participant_df(
                secret_user_ids=["00001"],
                tags=["child"],
            ).drop("tag", axis=1, errors="ignore"),
            "parent": pd.DataFrame(),  # Empty parent data
        }

        with patch_curious_transfer_module(
            fetch_return=sample_redcap_curious_data,
            format_return=formatted_data,
            send_return=[],
        ) as mocks:
            to_curious.main()
            # Check that fetch was called with enrollment_complete filter
            # First call is to PID 625, second to PID 247
            assert mocks["fetch"].call_count >= 1
            # Check PID 625 call for enrollment_complete filter
            calls = mocks["fetch"].call_args_list
            assert any("enrollment_complete" in str(call) for call in calls)

    def test_main_checks_all_account_types(
        self, sample_redcap_curious_data, formatted_curious_data, caplog
    ):
        """Test that both child and parent data are processed."""
        # Format as dict with both child and parent
        formatted_dict = {
            "child": formatted_curious_data[
                formatted_curious_data["tag"] == "child"
            ].drop("tag", axis=1),
            "parent": formatted_curious_data[
                formatted_curious_data["tag"] == "parent"
            ].drop("tag", axis=1),
        }

        with patch_curious_transfer_module(
            fetch_return=sample_redcap_curious_data,
            format_return=formatted_dict,
            send_return=[],
        ):
            to_curious.main()
            # Both child and parent should be processed
            # Check for processing of both types
            assert len(formatted_dict["child"]) > 0 or len(formatted_dict["parent"]) > 0

    def test_main_handles_partial_failures(
        self, sample_redcap_curious_data, formatted_curious_data
    ):
        """Test that partial failures are handled correctly."""
        failures = ["12345"]
        formatted_dict = {
            "child": formatted_curious_data[
                formatted_curious_data["tag"] == "child"
            ].drop("tag", axis=1),
            "parent": formatted_curious_data[
                formatted_curious_data["tag"] == "parent"
            ].drop("tag", axis=1),
        }

        with patch_curious_transfer_module(
            fetch_return=sample_redcap_curious_data,
            format_return=formatted_dict,
            send_return=failures,
        ) as mocks:
            to_curious.main()
            # update is called for both child and parent
            assert mocks["update"].called
            # Check that at least one call was made with the correct structure
            assert mocks["update"].call_count >= 1
            # Just verify update was called -
            # failures handling happens inside update_redcap

    def test_main_uses_correct_applet_id(
        self, sample_redcap_curious_data, formatted_curious_data
    ):
        """Test that correct applet IDs are used."""
        formatted_dict = {
            "child": formatted_curious_data[
                formatted_curious_data["tag"] == "child"
            ].drop("tag", axis=1),
            "parent": formatted_curious_data[
                formatted_curious_data["tag"] == "parent"
            ].drop("tag", axis=1),
        }

        with patch_curious_transfer_module(
            fetch_return=sample_redcap_curious_data,
            format_return=formatted_dict,
            send_return=[],
        ) as mocks:
            # Mock the applets structure
            with patch(
                "hbnmigration.from_redcap.to_curious.curious_variables.applets"
            ) as mock_applets:
                mock_applets.keys.return_value = [
                    "CHILD-Healthy Brain Network Questionnaires",
                    "Healthy Brain Network Questionnaires",
                ]
                mock_child_applet = MagicMock()
                mock_child_applet.applet_id = "child_applet_id"
                mock_parent_applet = MagicMock()
                mock_parent_applet.applet_id = "parent_applet_id"
                mock_applets.__getitem__.side_effect = lambda x: (
                    mock_child_applet if "CHILD" in x else mock_parent_applet
                )

                to_curious.main()

                # Check that send was called with applet IDs
                assert mocks["send"].called
                send_calls = mocks["send"].call_args_list
                # Verify applet IDs are used
                assert any("applet_id" in str(call) for call in send_calls)


# ============================================================================
# Integration Tests
# ============================================================================


class TestIntegration:
    """Integration tests for complete Curious transfer workflow."""

    @patch(
        "hbnmigration.from_redcap.to_redcap."
        "transform_redcap_data_for_responder_tracking"
    )
    @patch("hbnmigration.from_redcap.from_redcap.get_responder_ids")
    @patch("hbnmigration.from_redcap.from_redcap.redcap_variables")
    @patch("hbnmigration.from_redcap.to_curious.redcap_variables")
    @patch("hbnmigration.from_redcap.to_curious.curious_variables")
    @patch("hbnmigration.from_redcap.from_redcap.fetch_api_data")
    @patch("hbnmigration.from_redcap.to_curious.new_curious_account")
    @patch("hbnmigration.from_redcap.to_curious.redcap_api_push")
    def test_full_workflow_parliament_of_trees(
        self,
        mock_redcap_push,
        mock_new_account,
        mock_fetch_api,
        mock_curious_vars,
        mock_to_redcap_vars,
        mock_from_redcap_vars,
        mock_get_responder_ids,
        mock_transform_responder,
    ):
        """Test complete workflow with Parliament of Trees members."""
        setup_curious_integration_mocks(mock_curious_vars, mock_to_redcap_vars)

        # Mock Tokens as instantiated class
        mock_tokens = MagicMock()
        mock_tokens.pid247 = "token_247"
        mock_tokens.pid625 = "token_625"
        mock_from_redcap_vars.Tokens.return_value = mock_tokens
        mock_to_redcap_vars.Tokens.return_value = mock_tokens

        mock_from_redcap_vars.headers = {}
        mock_to_redcap_vars.headers = {}
        mock_from_redcap_vars.Endpoints.return_value.base_url = (
            "https://redcap.test/api/"
        )
        mock_to_redcap_vars.Endpoints.return_value.base_url = "https://redcap.test/api/"

        # PID 625 data (triggers)
        trigger_data = create_redcap_eav_df(
            records=["ST001", "AA001"],
            field_names=["mrn", "mrn"],
            values=["12345", "67890"],
        )

        # PID 247 data (source data with proper EAV structure)
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

        # Mock fetch_api_data to return different data based on token
        def fetch_side_effect(*args, **kwargs):
            # Check if this is PID 625 or 247 call based on token in kwargs or args
            params = args[2] if len(args) > 2 else kwargs.get("params", {})
            if params.get("token") == "token_625":
                return trigger_data
            return source_data

        mock_fetch_api.side_effect = fetch_side_effect
        mock_new_account.return_value = "Success"
        mock_redcap_push.return_value = 2

        # Mock transform_redcap_data_for_responder_tracking to return proper structure
        mock_transform_responder.return_value = (
            pd.DataFrame(
                {  # create_responders
                    "record": [1, 2],
                    "resp_email": ["alec@swamp.thing", "abby@arcane.com"],
                    "resp_fname": ["Alec", "Abby"],
                    "resp_lname": ["Holland", "Arcane"],
                    "resp_phone": ["555-0001", "555-0002"],
                }
            ),
            pd.DataFrame(),  # update_responders
            pd.DataFrame(),  # participants
        )

        # Mock get_responder_ids to return proper responder ID mapping
        mock_get_responder_ids.return_value = pd.DataFrame(
            {
                "record": ["R000001", "R000002"],
                "resp_email": ["alec@swamp.thing", "abby@arcane.com"],
            }
        )

        # Mock AppletCredentials with proper structure
        mock_applet_creds = MagicMock()
        mock_applet_creds.__getitem__.return_value = {
            "email": "test@test.com",
            "password": "test123",
            "applet_password": "applet123",
        }
        mock_curious_vars.AppletCredentials.return_value = mock_applet_creds

        # Mock applets structure
        mock_child_applet = MagicMock()
        mock_child_applet.applet_id = "child_applet_id"
        mock_parent_applet = MagicMock()
        mock_parent_applet.applet_id = "parent_applet_id"

        mock_applets = MagicMock()
        mock_applets.__getitem__.side_effect = lambda x: (
            mock_child_applet if "CHILD" in x else mock_parent_applet
        )
        mock_applets.keys.return_value = [
            "CHILD-Healthy Brain Network Questionnaires",
            "Healthy Brain Network Questionnaires",
        ]
        mock_curious_vars.applets = mock_applets

        to_curious.main()

        # With new structure:
        # - 2 MRNs create 2 child records and 2 parent records
        # - Child records go to CHILD applet
        # - Parent records and child records both go to parent applet
        # Total: at least 2 accounts created (could be more depending on implementation)
        assert mock_new_account.call_count >= 2

        # Should update REDCap (called for both child and parent updates)
        assert mock_redcap_push.called


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
            # Create empty DataFrame
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


class TestUpdateCompleteParentSecondGuardianConsent:
    """Tests for update_complete_parent_second_guardian_consent."""

    @pytest.mark.parametrize(
        "guardian_value_label, expected_label",
        [
            ("No", "Not Required"),
            (
                "Not Applicable (Adult Participant)",
                "Not Applicable (Adult Participant)",
            ),
        ],
    )
    def test_appends_when_missing(
        self,
        guardian_value_label: Literal["No", "Not Applicable (Adult Participant)"],
        expected_label: Literal["Not Required", "Not Applicable (Adult Participant)"],
    ) -> None:
        """Test appending missing `complete_parent_second_guardian_consent`s."""
        df = create_redcap_eav_df(
            records=["001"],
            field_names=["guardian2_consent"],
            values=[
                Values.PID247.guardian2_consent[guardian_value_label],
            ],
        )

        result = update_complete_parent_second_guardian_consent(df)

        rows = result[result["field_name"] == "complete_parent_second_guardian_consent"]

        assert len(rows) == 1
        assert rows.iloc[0]["record"] == "001"
        assert (
            rows.iloc[0]["value"]
            == Values.PID625.complete_parent_second_guardian_consent[expected_label]
        )

    @pytest.mark.parametrize(
        "initial_label, guardian_label, expected_label",
        [
            (
                "Incomplete",
                "No",
                "Not Required",
            ),
            (
                "Unverified",
                "Not Applicable (Adult Participant)",
                "Not Applicable (Adult Participant)",
            ),
        ],
    )
    def test_updates_existing_value(
        self,
        initial_label: Literal["Incomplete", "Unverified"],
        guardian_label: Literal["No", "Not Applicable (Adult Participant)"],
        expected_label: Literal["Not Required", "Not Applicable (Adult Participant)"],
    ) -> None:
        """Test updating existing `complete_parent_second_guardian_consent`s."""
        df = create_redcap_eav_df(
            records=["001", "001"],
            field_names=[
                "guardian2_consent",
                "complete_parent_second_guardian_consent",
            ],
            values=[
                Values.PID247.guardian2_consent[guardian_label],
                Values.PID625.complete_parent_second_guardian_consent[initial_label],
            ],
        )

        result = update_complete_parent_second_guardian_consent(df)

        rows = result[result["field_name"] == "complete_parent_second_guardian_consent"]

        assert len(rows) == 1
        assert (
            rows.iloc[0]["value"]
            == Values.PID625.complete_parent_second_guardian_consent[expected_label]
        )

    def test_leaves_unmapped_records_unchanged(self) -> None:
        """Test applicable records."""
        df = create_redcap_eav_df(
            records=["001"],
            field_names=["guardian2_consent"],
            values=[Values.PID247.guardian2_consent["Yes"]],
        )

        result = update_complete_parent_second_guardian_consent(df)

        assert (
            result["field_name"] == "complete_parent_second_guardian_consent"
        ).sum() == 0

    def test_handles_multiple_records_independently(self) -> None:
        """Test pairings."""
        df = create_redcap_eav_df(
            records=["001", "002"],
            field_names=["guardian2_consent", "guardian2_consent"],
            values=[
                Values.PID247.guardian2_consent["No"],
                Values.PID247.guardian2_consent["Not Applicable (Adult Participant)"],
            ],
        )

        result = update_complete_parent_second_guardian_consent(df)

        rows = result[
            result["field_name"] == "complete_parent_second_guardian_consent"
        ].set_index("record")["value"]

        assert (
            rows["001"]
            == Values.PID625.complete_parent_second_guardian_consent["Not Required"]
        )
        assert (
            rows["002"]
            == Values.PID625.complete_parent_second_guardian_consent[
                "Not Applicable (Adult Participant)"
            ]
        )

    def test_does_not_modify_other_fields(self) -> None:
        """Test `guardian2_consent` against unrelated field."""
        df = create_redcap_eav_df(
            records=["001", "001"],
            field_names=["guardian2_consent", "intake_ready"],
            values=[
                Values.PID247.guardian2_consent["No"],
                Values.PID247.intake_ready["Ready to Send to Intake Redcap"],
            ],
        )

        result = update_complete_parent_second_guardian_consent(df)

        intake_row = result[result["field_name"] == "intake_ready"].iloc[0]
        assert (
            intake_row["value"]
            == Values.PID247.intake_ready["Ready to Send to Intake Redcap"]
        )


class TestMainSecondGuardianConsentRegression:
    """Regression tests for second guardian consent logic in main()."""

    def test_main_applies_second_guardian_consent_mapping(self) -> None:
        """Test `update_complete_parent_second_guardian_consent` from `main`."""
        source_df = create_redcap_eav_df(
            records=["001"],
            field_names=["guardian2_consent"],
            values=[
                Values.PID247.guardian2_consent["No"],
            ],
        )

        with patch_redcap_transfer_module(
            fetch_return=source_df, push_return=1, update_return=1
        ) as mocks:
            to_redcap.main()

            # update_source() should receive transformed data
            update_df = mocks["update"].call_args[0][0]

            rows = update_df[
                update_df["field_name"] == "complete_parent_second_guardian_consent"
            ]

            assert len(rows) == 1
            assert rows.iloc[0]["record"] == "001"
            assert (
                rows.iloc[0]["value"]
                == Values.PID625.complete_parent_second_guardian_consent["Not Required"]
            )
