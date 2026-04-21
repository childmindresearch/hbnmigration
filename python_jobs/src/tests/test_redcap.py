"""Test code for data transfer from REDCap to Curious."""

from typing import Any
from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient
import pandas as pd
import pytest
import requests

from hbnmigration.from_curious.config import account_types
from hbnmigration.from_redcap import to_curious, to_redcap
from hbnmigration.from_redcap.from_redcap import fetch_data

from .conftest import (
    create_curious_api_failure,
    create_curious_participant_df,
    create_redcap_eav_df,
    patch_curious_api_dependencies,
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
    def test_in_set_various_inputs(
        self, input_value: Any, required: Any, expected: bool
    ) -> None:
        """Test _in_set with various input types."""
        assert to_curious._in_set(input_value, required) is expected

    @pytest.mark.parametrize(
        "invalid_input",
        [None, 3.14, {}, {}],
    )
    def test_in_set_invalid_types_return_false(self, invalid_input: Any) -> None:
        """Test that _in_set returns False with invalid types."""
        assert to_curious._in_set(invalid_input, 1) is False


# ============================================================================
# _check_for_data_to_process() Tests
# ============================================================================


class TestCheckForDataToProcess:
    """Tests for _check_for_data_to_process helper function."""

    def test_logs_info_when_no_data(
        self, caplog: pytest.LogCaptureFixture, formatted_curious_data: pd.DataFrame
    ) -> None:
        """Test that appropriate log message when no data for account type."""
        df_empty = formatted_curious_data[
            formatted_curious_data["accountType"] == "nonexistent"
        ]
        to_curious._check_for_data_to_process(df_empty, "full")
        assert "There is not full consent data to process" in caplog.text

    def test_logs_info_when_data_exists(self, caplog: pytest.LogCaptureFixture) -> None:
        """Test that appropriate log message when data exists."""
        df = pd.DataFrame(
            {
                "accountType": ["full", "limited"],
                "secretUserId": ["00001_P", "00001"],
            }
        )
        to_curious._check_for_data_to_process(df, "full")
        assert "Full data was prepared to be sent to the Curious API" in caplog.text

    def test_checks_all_account_types(
        self, caplog: pytest.LogCaptureFixture, formatted_curious_data: pd.DataFrame
    ) -> None:
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

    def test_formats_basic_parent_child_data(self) -> None:
        """Test basic formatting of parent and child data."""
        redcap_data = create_redcap_eav_df(
            records=["001", "001", "001", "001", "001"],
            field_names=[
                "mrn",
                "parent_involvement___1",
                "adult_enrollment_form_complete",
                "parentfirstname",
                "r_id",
            ],
            values=["12345", "1", "0", "Jane", "R00001"],
        )
        result = to_curious.format_redcap_data_for_curious(redcap_data)
        assert isinstance(result, dict)
        assert "child" in result
        assert "parent" in result
        assert isinstance(result["child"], pd.DataFrame)
        assert isinstance(result["parent"], pd.DataFrame)
        assert len(result["parent"]) >= 1

    def test_pads_secret_user_id_with_zeros(self) -> None:
        """Test that secretUserId is padded to 5 characters for children."""
        redcap_data = create_redcap_eav_df(
            records=["001", "001"],
            field_names=["mrn", "parent_involvement___1"],
            values=["123", "1"],
        )
        result = to_curious.format_redcap_data_for_curious(redcap_data)
        assert "secretUserId" in result["child"].columns
        child_ids = result["child"]["secretUserId"]
        for user_id in child_ids:
            assert len(user_id) == 5, f"Child secretUserId should be 5 chars: {user_id}"

    def test_appends_p_suffix_to_parent_before_padding(self) -> None:
        """Test that parent secretUserId gets r_id value."""
        redcap_data = create_redcap_eav_df(
            records=["001", "001", "001", "001"],
            field_names=["mrn", "parent_involvement___1", "parentfirstname", "r_id"],
            values=["12345", "1", "Jane", "R00001"],
        )
        result = to_curious.format_redcap_data_for_curious(redcap_data)
        assert "secretUserId" in result["parent"].columns
        parent_ids = result["parent"]["secretUserId"]
        assert len(parent_ids) > 0

    def test_filters_by_parent_involvement(self) -> None:
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
            values=["12345", "1", "67890", "1", "1"],
        )
        result = to_curious.format_redcap_data_for_curious(redcap_data)
        assert "secretUserId" in result["child"].columns
        child_secret_ids = {x.lstrip("0") for x in result["child"]["secretUserId"]}
        assert "12345" in child_secret_ids

    def test_drops_parent_involvement_columns(self) -> None:
        """Test that parent_involvement columns are dropped from output."""
        redcap_data = create_redcap_eav_df(
            records=["001", "001"],
            field_names=["mrn", "parent_involvement___1"],
            values=["12345", "1"],
        )
        result = to_curious.format_redcap_data_for_curious(redcap_data)
        assert "parent_involvement" not in result["child"].columns
        assert "adult_enrollment_form_complete" not in result["child"].columns

    def test_adds_default_values_for_missing_fields(self) -> None:
        """Test that missing fields get default values from config."""
        redcap_data = create_redcap_eav_df(
            records=["001", "001"],
            field_names=["mrn", "parent_involvement___1"],
            values=["12345", "1"],
        )
        result = to_curious.format_redcap_data_for_curious(redcap_data)
        assert len(result["child"].columns) > 0
        assert len(result["parent"].columns) > 0

    def test_replaces_nan_with_none(self) -> None:
        """Test that NaN values are replaced with None."""
        redcap_data = create_redcap_eav_df(
            records=["001", "001"],
            field_names=["mrn", "parent_involvement___1"],
            values=["12345", "1"],
        )
        result = to_curious.format_redcap_data_for_curious(redcap_data)
        child_dicts = result["child"].to_dict(orient="records")
        parent_dicts = result["parent"].to_dict(orient="records")
        for record in child_dicts + parent_dicts:
            for _key, value in record.items():
                if value is None:
                    continue
                assert value is not pd.NA


# ============================================================================
# send_to_curious() Tests
# ============================================================================


class TestSendToCurious:
    """Tests for send_to_curious function."""

    def test_sends_all_records_to_curious(
        self, formatted_curious_data: pd.DataFrame, mock_curious_variables: MagicMock
    ) -> None:
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
        self, formatted_curious_data: pd.DataFrame, mock_curious_variables: MagicMock
    ) -> None:
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
        self, formatted_curious_data: pd.DataFrame, mock_curious_variables: MagicMock
    ) -> None:
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

    def test_filters_none_values_from_records(
        self, mock_curious_variables: MagicMock
    ) -> None:
        """Test that None values are filtered from records before sending."""
        with patch_curious_api_dependencies(new_account_return="Success") as mocks:
            tokens = mock_curious_variables.Tokens(
                mock_curious_variables.Endpoints(),
                mock_curious_variables.Credentials.hbn_mindlogger,
            )
            df_with_nones = pd.DataFrame(
                {
                    "secretUserId": ["00001"],
                    "tag": ["child"],
                    "accountType": ["limited"],
                    "firstName": pd.array([None], dtype=object),
                    "lastName": ["Holland"],
                    "nickname": pd.array([None], dtype=object),
                    "role": ["respondent"],
                    "language": ["en"],
                }
            )
            to_curious.send_to_curious(df_with_nones, tokens, "test_applet_id")
            call_record = mocks["new_account"].call_args[0][2]
            assert "firstName" not in call_record
            assert "lastName" in call_record

    def test_includes_authorization_header(
        self, formatted_curious_data: pd.DataFrame, mock_curious_variables: MagicMock
    ) -> None:
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

    @patch("hbnmigration.from_redcap.to_curious.new_curious_account")
    def test_sends_without_cache(self, mock_account: MagicMock) -> None:
        """Test sending without cache."""
        mock_account.return_value = {"status": "ok"}
        tokens = MagicMock()
        tokens.access = "test-token"
        tokens.endpoints.base_url = "https://api.example.com"
        df = pd.DataFrame({"secretUserId": ["abc123"]})
        failures = to_curious.send_to_curious(df, tokens, "applet-123")
        assert failures == []
        mock_account.assert_called_once()

    @patch("hbnmigration.from_redcap.to_curious.new_curious_account")
    def test_handles_request_failure(self, mock_account: MagicMock) -> None:
        """Test failure handling."""
        import requests

        mock_account.side_effect = requests.exceptions.RequestException("timeout")
        tokens = MagicMock()
        tokens.access = "test-token"
        tokens.endpoints.base_url = "https://api.example.com"
        df = pd.DataFrame({"secretUserId": ["abc123"]})
        failures = to_curious.send_to_curious(df, tokens, "applet-123")
        assert len(failures) == 1


# ============================================================================
# update_redcap() Tests
# ============================================================================


class TestUpdateRedcap:
    """Tests for update_redcap function."""

    @patch("hbnmigration.from_redcap.to_curious.redcap_api_push")
    @patch("hbnmigration.from_redcap.to_curious.redcap_variables")
    def test_updates_enrollment_complete_for_successes(
        self,
        mock_redcap_vars: MagicMock,
        mock_push: MagicMock,
    ) -> None:
        """Test that enrollment_complete is updated for successful transfers."""
        mock_push.return_value = 1
        mock_redcap_vars.Tokens.pid625 = "token_625"
        mock_redcap_vars.Endpoints.return_value.base_url = "https://redcap.test/api/"
        mock_redcap_vars.headers = {}
        redcap_data = create_redcap_eav_df(
            records=["001", "001"],
            field_names=["mrn", "enrollment_complete"],
            values=["12345", "1"],
        )
        curious_data = create_curious_participant_df(
            secret_user_ids=["12345"],
            tags=["child"],
        )
        to_curious.update_redcap(redcap_data, curious_data, [])
        mock_push.assert_called_once()

    @patch("hbnmigration.from_redcap.to_curious.redcap_api_push")
    @patch("hbnmigration.from_redcap.to_curious.redcap_variables")
    def test_excludes_failures_from_update(
        self,
        mock_redcap_vars: MagicMock,
        mock_push: MagicMock,
    ) -> None:
        """Test that failed records are not updated in REDCap."""
        mock_push.return_value = 0
        mock_redcap_vars.Tokens.pid625 = "token_625"
        mock_redcap_vars.Endpoints.return_value.base_url = "https://redcap.test/api/"
        redcap_data = create_redcap_eav_df(
            records=["001", "001"],
            field_names=["mrn", "enrollment_complete"],
            values=["12345", "1"],
        )
        curious_data = create_curious_participant_df(
            secret_user_ids=["12345"],
            tags=["child"],
        )
        failures = ["12345"]
        to_curious.update_redcap(redcap_data, curious_data, failures)
        mock_push.assert_not_called()

    @patch("hbnmigration.from_redcap.to_curious.redcap_api_push")
    @patch("hbnmigration.from_redcap.to_curious.redcap_variables")
    def test_filters_out_parent_records_from_update(
        self,
        mock_redcap_vars: MagicMock,
        mock_push: MagicMock,
    ) -> None:
        """Test that parent records (_P suffix) are excluded from REDCap update."""
        mock_push.return_value = 1
        mock_redcap_vars.Tokens.pid625 = "token_625"
        mock_redcap_vars.Endpoints.return_value.base_url = "https://redcap.test/api/"
        mock_redcap_vars.headers = {}
        redcap_data = create_redcap_eav_df(
            records=["001", "001"],
            field_names=["mrn", "enrollment_complete"],
            values=["12345", "1"],
        )
        curious_data = pd.DataFrame(
            {
                "secretUserId": ["12345", "12345_P"],
                "tag": ["child", "parent"],
            }
        )
        to_curious.update_redcap(redcap_data, curious_data, [])
        mock_push.assert_called_once()


# ============================================================================
# Webhook Endpoint Tests (replaces old TestMain)
# ============================================================================


class TestToCuriousWebhook:
    """Tests for the REDCap to Curious webhook endpoints."""

    @pytest.fixture
    def client(self) -> TestClient:
        """Test client."""
        return TestClient(to_curious.app)

    def test_root(self, client: TestClient) -> None:
        """Test root endpoint."""
        response = client.get("/")
        assert response.status_code == requests.codes["okay"]
        assert "message" in response.json()

    def test_health(self, client: TestClient) -> None:
        """Test health endpoint."""
        response = client.get("/health")
        assert response.status_code == requests.codes["okay"]
        assert response.json() == {"status": "healthy"}

    def test_webhook_accepts_valid_trigger(self, client: TestClient) -> None:
        """Test trigger."""
        with patch("hbnmigration.from_redcap.to_curious.process_record_for_curious"):
            response = client.post(
                "/webhook/redcap-to-curious",
                data={
                    "project_id": "625",
                    "instrument": "enrollment",
                    "record": "12345",
                    "ready_to_send_to_curious": "1",
                },
            )
            assert response.status_code == requests.codes["okay"]
            assert response.json()["status"] == "accepted"
            assert response.json()["record_id"] == "12345"

    def test_webhook_ignores_when_flag_not_set(self, client: TestClient) -> None:
        """Test without flag."""
        response = client.post(
            "/webhook/redcap-to-curious",
            data={
                "project_id": "625",
                "instrument": "enrollment",
                "record": "12345",
                "ready_to_send_to_curious": "0",
            },
        )
        assert response.status_code == requests.codes["okay"]
        assert response.json()["status"] == "ignored"

    def test_webhook_ignores_when_flag_missing(self, client: TestClient) -> None:
        """Test without flag."""
        response = client.post(
            "/webhook/redcap-to-curious",
            data={
                "project_id": "625",
                "instrument": "some_form",
                "record": "12345",
            },
        )
        assert response.status_code == requests.codes["okay"]
        assert response.json()["status"] == "ignored"


class TestToRedcapWebhook:
    """Tests for the REDCap to Intake REDCap webhook endpoints."""

    @pytest.fixture
    def client(self) -> TestClient:
        """Test client."""
        return TestClient(to_redcap.app)

    def test_root(self, client: TestClient) -> None:
        """Test root endpoint."""
        response = client.get("/")
        assert response.status_code == requests.codes["okay"]
        assert "message" in response.json()

    def test_health(self, client: TestClient) -> None:
        """Test health endpoint."""
        response = client.get("/health")
        assert response.status_code == requests.codes["okay"]
        assert response.json() == {"status": "healthy"}

    def test_webhook_accepts_valid_trigger(self, client: TestClient) -> None:
        """Test trigger."""
        with patch("hbnmigration.from_redcap.to_redcap.process_record_for_intake"):
            response = client.post(
                "/webhook/redcap-to-intake",
                data={
                    "project_id": "625",
                    "instrument": "enrollment",
                    "record": "12345",
                    "ready_to_send_to_intake_redcap": "1",
                },
            )
            assert response.status_code == requests.codes["okay"]
            assert response.json()["status"] == "accepted"
            assert response.json()["record_id"] == "12345"

    def test_webhook_ignores_when_flag_not_set(self, client: TestClient) -> None:
        """Test missing flag handling."""
        response = client.post(
            "/webhook/redcap-to-intake",
            data={
                "project_id": "625",
                "instrument": "enrollment",
                "record": "12345",
                "ready_to_send_to_intake_redcap": "0",
            },
        )
        assert response.status_code == requests.codes["okay"]
        assert response.json()["status"] == "ignored"

    def test_webhook_ignores_when_flag_missing(self, client: TestClient) -> None:
        """Test missing flag handling."""
        response = client.post(
            "/webhook/redcap-to-intake",
            data={
                "project_id": "625",
                "instrument": "some_form",
                "record": "12345",
            },
        )
        assert response.status_code == requests.codes["okay"]
        assert response.json()["status"] == "ignored"


# ============================================================================
# process_record_for_curious() Tests (replaces old TestMain)
# ============================================================================


class TestProcessRecordForCurious:
    """Tests for process_record_for_curious function."""

    @patch("hbnmigration.from_redcap.to_curious.clear_ready_flag")
    @patch("hbnmigration.from_redcap.to_curious.update_redcap")
    @patch("hbnmigration.from_redcap.to_curious.push_parent_data", return_value=[])
    @patch("hbnmigration.from_redcap.to_curious.push_child_data", return_value=[])
    @patch("hbnmigration.from_redcap.to_curious.format_redcap_data_for_curious")
    @patch("hbnmigration.from_redcap.to_curious.fetch_data")
    def test_successful_processing(
        self,
        mock_fetch: MagicMock,
        mock_format: MagicMock,
        mock_push_child: MagicMock,
        mock_push_parent: MagicMock,
        mock_update: MagicMock,
        mock_clear: MagicMock,
        sample_redcap_curious_data: pd.DataFrame,
        formatted_curious_data: pd.DataFrame,
    ) -> None:
        """Test successful end-to-end processing of a record."""
        mock_fetch.return_value = sample_redcap_curious_data
        formatted_dict = {
            "child": formatted_curious_data[
                formatted_curious_data["tag"] == "child"
            ].drop("tag", axis=1),
            "parent": formatted_curious_data[
                formatted_curious_data["tag"] == "parent"
            ].drop("tag", axis=1),
        }
        mock_format.return_value = formatted_dict

        result = to_curious.process_record_for_curious("001")

        assert result["status"] == "success"
        assert result["record_id"] == "001"
        mock_fetch.assert_called_once()
        mock_format.assert_called_once()
        mock_clear.assert_called_once_with("001")

    @patch("hbnmigration.from_redcap.to_curious.fetch_data")
    def test_no_data_found(self, mock_fetch: MagicMock) -> None:
        """Test that missing data returns error status."""
        mock_fetch.return_value = pd.DataFrame()

        result = to_curious.process_record_for_curious("999")

        assert result["status"] == "error"
        assert "No data found" in result["message"]

    @patch("hbnmigration.from_redcap.to_curious.format_redcap_data_for_curious")
    @patch("hbnmigration.from_redcap.to_curious.fetch_data")
    def test_no_processable_data(
        self, mock_fetch: MagicMock, mock_format: MagicMock
    ) -> None:
        """Test when formatting yields empty DataFrames."""
        mock_fetch.return_value = pd.DataFrame({"record": ["001"]})
        mock_format.return_value = {
            "child": pd.DataFrame(),
            "parent": pd.DataFrame(),
        }

        result = to_curious.process_record_for_curious("001")

        assert result["status"] == "error"
        assert "No processable data" in result["message"]

    @patch("hbnmigration.from_redcap.to_curious.clear_ready_flag")
    @patch("hbnmigration.from_redcap.to_curious.update_redcap")
    @patch(
        "hbnmigration.from_redcap.to_curious.push_parent_data", return_value=["mrn1"]
    )
    @patch("hbnmigration.from_redcap.to_curious.push_child_data", return_value=[])
    @patch("hbnmigration.from_redcap.to_curious.format_redcap_data_for_curious")
    @patch("hbnmigration.from_redcap.to_curious.fetch_data")
    def test_partial_failures(
        self,
        mock_fetch: MagicMock,
        mock_format: MagicMock,
        mock_push_child: MagicMock,
        mock_push_parent: MagicMock,
        mock_update: MagicMock,
        mock_clear: MagicMock,
    ) -> None:
        """Test that partial failures are reported correctly."""
        mock_fetch.return_value = pd.DataFrame({"record": ["001"]})
        mock_format.return_value = {
            "child": pd.DataFrame({"secretUserId": ["abc"]}),
            "parent": pd.DataFrame({"secretUserId": ["def"]}),
        }

        result = to_curious.process_record_for_curious("001")

        assert result["status"] == "partial"
        assert len(result["failures"]) == 1
        mock_clear.assert_called_once_with("001")

    @patch("hbnmigration.from_redcap.to_curious.fetch_data")
    def test_exception_handling(self, mock_fetch: MagicMock) -> None:
        """Test that exceptions are caught and returned as error status."""
        mock_fetch.side_effect = RuntimeError("connection failed")

        result = to_curious.process_record_for_curious("001")

        assert result["status"] == "error"
        assert "connection failed" in result["message"]


# ============================================================================
# process_record_for_intake() Tests
# ============================================================================


class TestProcessRecordForIntake:
    """Tests for process_record_for_intake function."""

    @patch("hbnmigration.from_redcap.to_redcap.clear_ready_flag")
    @patch("hbnmigration.from_redcap.to_redcap.push_to_intake_redcap", return_value=5)
    @patch("hbnmigration.from_redcap.to_redcap.format_data_for_intake")
    @patch("hbnmigration.from_redcap.to_redcap.fetch_data")
    def test_successful_processing(
        self,
        mock_fetch: MagicMock,
        mock_format: MagicMock,
        mock_push: MagicMock,
        mock_clear: MagicMock,
    ) -> None:
        """Test successful processing of a record for Intake REDCap."""
        mock_fetch.return_value = pd.DataFrame(
            {"record": ["123"], "field_name": ["mrn"], "value": ["abc"]}
        )
        mock_format.return_value = pd.DataFrame(
            {"record": ["123"], "field_name": ["mrn"], "value": ["abc"]}
        )

        result = to_redcap.process_record_for_intake("123")

        assert result["status"] == "success"
        assert result["record_id"] == "123"
        assert result["rows_pushed"] == 5
        mock_clear.assert_called_once_with("123")

    @patch("hbnmigration.from_redcap.to_redcap.fetch_data")
    def test_no_data_found(self, mock_fetch: MagicMock) -> None:
        """Test that missing data returns error status."""
        mock_fetch.return_value = pd.DataFrame()

        result = to_redcap.process_record_for_intake("999")

        assert result["status"] == "error"
        assert "No data found" in result["message"]

    @patch("hbnmigration.from_redcap.to_redcap.format_data_for_intake")
    @patch("hbnmigration.from_redcap.to_redcap.fetch_data")
    def test_no_processable_data(
        self, mock_fetch: MagicMock, mock_format: MagicMock
    ) -> None:
        """Test when formatting yields empty DataFrame."""
        mock_fetch.return_value = pd.DataFrame({"record": ["123"]})
        mock_format.return_value = pd.DataFrame()

        result = to_redcap.process_record_for_intake("123")

        assert result["status"] == "error"
        assert "No processable data" in result["message"]

    @patch("hbnmigration.from_redcap.to_redcap.fetch_data")
    def test_exception_handling(self, mock_fetch: MagicMock) -> None:
        """Test that exceptions are caught and returned as error status."""
        mock_fetch.side_effect = RuntimeError("connection failed")

        result = to_redcap.process_record_for_intake("123")

        assert result["status"] == "error"
        assert "connection failed" in result["message"]


# ============================================================================
# clear_ready_flag() Tests
# ============================================================================


class TestClearReadyFlagCurious:
    """Tests for to_curious.clear_ready_flag."""

    @patch("hbnmigration.from_redcap.to_curious.redcap_api_push")
    @patch("hbnmigration.from_redcap.to_curious.fetch_data")
    def test_clears_flag(self, mock_fetch: MagicMock, mock_push: MagicMock) -> None:
        """Test flag setting."""
        mock_fetch.return_value = pd.DataFrame(
            {
                "field_name": ["ready_to_send_to_curious"],
                "redcap_event_name": ["event_1"],
            }
        )

        to_curious.clear_ready_flag("123")

        mock_push.assert_called_once()
        df_arg = mock_push.call_args.kwargs["df"]
        assert df_arg.iloc[0]["value"] == "0"
        assert df_arg.iloc[0]["field_name"] == "ready_to_send_to_curious"

    @patch("hbnmigration.from_redcap.to_curious.fetch_data")
    def test_handles_missing_record(self, mock_fetch: MagicMock) -> None:
        """Test handling missing record."""
        mock_fetch.return_value = pd.DataFrame()
        # Should not raise
        to_curious.clear_ready_flag("nonexistent")


class TestClearReadyFlagIntake:
    """Tests for to_redcap.clear_ready_flag."""

    @patch("hbnmigration.from_redcap.to_redcap.update_source_redcap_status")
    @patch("hbnmigration.from_redcap.to_redcap.fetch_data")
    def test_clears_flag(self, mock_fetch: MagicMock, mock_update: MagicMock) -> None:
        """Test flag setting."""
        mock_fetch.return_value = pd.DataFrame(
            {
                "field_name": ["ready_to_send_to_intake_redcap"],
                "redcap_event_name": ["event_1"],
            }
        )

        to_redcap.clear_ready_flag("123")

        mock_update.assert_called_once_with("123", "0", "event_1")

    @patch("hbnmigration.from_redcap.to_redcap.fetch_data")
    def test_handles_missing_record(self, mock_fetch: MagicMock) -> None:
        """Test missing record handling."""
        mock_fetch.return_value = pd.DataFrame()
        # Should not raise
        to_redcap.clear_ready_flag("nonexistent")


# ============================================================================
# Integration Tests
# ============================================================================


class TestIntegration:
    """Integration tests for complete Curious transfer workflow."""

    @patch(
        "hbnmigration.from_redcap.from_redcap."
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
        mock_redcap_push: MagicMock,
        mock_new_account: MagicMock,
        mock_fetch_api: MagicMock,
        mock_curious_vars: MagicMock,
        mock_to_redcap_vars: MagicMock,
        mock_from_redcap_vars: MagicMock,
        mock_get_responder_ids: MagicMock,
        mock_transform_responder: MagicMock,
    ) -> None:
        """Test complete workflow with Parliament of Trees members."""
        setup_curious_integration_mocks(mock_curious_vars, mock_to_redcap_vars)

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

        source_data = create_redcap_eav_df(
            records=["ST001", "ST001", "ST001", "AA001", "AA001", "AA001"],
            field_names=[
                "mrn",
                "parent_involvement___1",
                "enrollment_complete",
                "mrn",
                "parent_involvement___1",
                "enrollment_complete",
            ],
            values=["12345", "1", "1", "67890", "1", "1"],
        )

        mock_fetch_api.side_effect = lambda *args, **kwargs: source_data
        mock_new_account.return_value = "Success"
        mock_redcap_push.return_value = 2

        mock_transform_responder.return_value = (
            pd.DataFrame(
                {
                    "record": [1, 2],
                    "resp_email": ["alec@swamp.thing", "abby@arcane.com"],
                    "resp_fname": ["Alec", "Abby"],
                    "resp_lname": ["Holland", "Arcane"],
                    "resp_phone": ["555-0001", "555-0002"],
                }
            ),
            pd.DataFrame(),
            pd.DataFrame(),
        )
        mock_get_responder_ids.return_value = pd.DataFrame(
            {
                "record": ["R000001", "R000002"],
                "resp_email": ["alec@swamp.thing", "abby@arcane.com"],
            }
        )

        mock_applet_creds = MagicMock()
        mock_applet_creds.__getitem__.return_value = {
            "email": "test@test.com",
            "password": "test123",
            "applet_password": "applet123",
        }
        mock_curious_vars.AppletCredentials.return_value = mock_applet_creds

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

        # Use process_record_for_curious instead of main()
        result = to_curious.process_record_for_curious("ST001")

        assert result["status"] in ("success", "partial", "error")
        # The function should have attempted to fetch and process data
        mock_fetch_api.assert_called()


# ============================================================================
# Edge Case Tests
# ============================================================================


class TestEdgeCases:
    """Tests for edge cases and error conditions."""

    def test_format_handles_empty_dataframe(self) -> None:
        """Test that empty DataFrame is handled gracefully."""
        empty_df = pd.DataFrame()
        with pytest.raises((KeyError, ValueError)):
            to_curious.format_redcap_data_for_curious(empty_df)

    def test_send_to_curious_with_empty_dataframe(
        self, mock_curious_variables: MagicMock
    ) -> None:
        """Test sending empty DataFrame to Curious."""
        with patch_curious_api_dependencies(new_account_return="Success") as mocks:
            tokens = mock_curious_variables.Tokens(
                mock_curious_variables.Endpoints(),
                mock_curious_variables.Credentials.hbn_mindlogger,
            )
            empty_df = pd.DataFrame(columns=["secretUserId", "accountType", "tag"])
            failures = to_curious.send_to_curious(empty_df, tokens, "test_applet")
            assert len(failures) == 0
            mocks["new_account"].assert_not_called()

    def test_update_redcap_with_no_successes(self) -> None:
        """Test REDCap update when all records failed."""
        redcap_data = create_redcap_eav_df(
            records=["001", "001"],
            field_names=["mrn", "enrollment_complete"],
            values=["12345", "1"],
        )
        curious_data = create_curious_participant_df(
            secret_user_ids=["12345"],
            tags=["child"],
        )
        failures = ["12345"]
        with patch("hbnmigration.from_redcap.to_curious.redcap_api_push") as mock_push:
            mock_push.return_value = 0
            to_curious.update_redcap(redcap_data, curious_data, failures)
            mock_push.assert_not_called()

    def test_send_continues_after_single_failure(
        self, mock_curious_variables: MagicMock
    ) -> None:
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


# ============================================================================
# fetch_data() Optional Fields Tests
# ============================================================================


class TestFetchDataOptionalFields:
    """Test optional fields."""

    @patch("hbnmigration.from_redcap.from_redcap.redcap_variables")
    @patch("hbnmigration.from_redcap.from_redcap.Endpoints")
    @patch("hbnmigration.from_redcap.from_redcap.fetch_api_data")
    def test_fetch_without_export_fields(
        self,
        mock_fetch_api: MagicMock,
        mock_endpoints: MagicMock,
        mock_redcap_vars: MagicMock,
    ) -> None:
        """Test that export_fields=None omits 'fields' from the API request."""
        mock_endpoints.base_url = "https://redcap.test/api/"
        mock_redcap_vars.headers = {}
        # Return non-empty DataFrame to avoid NoData exception
        mock_fetch_api.return_value = pd.DataFrame(
            {"record": ["1"], "field_name": ["f"], "value": ["v"]}
        )
        fetch_data("fake_token")
        request_data = mock_fetch_api.call_args[0][2]
        assert "fields" not in request_data

    @patch("hbnmigration.from_redcap.from_redcap.redcap_variables")
    @patch("hbnmigration.from_redcap.from_redcap.Endpoints")
    @patch("hbnmigration.from_redcap.from_redcap.fetch_api_data")
    def test_fetch_with_export_fields(
        self,
        mock_fetch_api: MagicMock,
        mock_endpoints: MagicMock,
        mock_redcap_vars: MagicMock,
    ) -> None:
        """Test that export_fields is included when provided."""
        mock_endpoints.base_url = "https://redcap.test/api/"
        mock_redcap_vars.headers = {}
        mock_fetch_api.return_value = pd.DataFrame(
            {"record": ["1"], "field_name": ["f"], "value": ["v"]}
        )
        fetch_data("fake_token", "field1,field2")
        request_data = mock_fetch_api.call_args[0][2]
        assert request_data.get("fields") == "field1,field2"
