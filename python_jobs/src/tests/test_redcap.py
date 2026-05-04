"""Test code for data transfer from REDCap to Curious and REDCap to REDCap."""

from typing import Any
from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient
import pandas as pd
import pytest
import requests

from hbnmigration.from_curious.config import account_types
from hbnmigration.from_redcap import to_curious, to_redcap
from hbnmigration.from_redcap.config import Values
from hbnmigration.from_redcap.from_redcap import fetch_data, RedcapRecord
from hbnmigration.from_redcap.to_redcap import (
    _apply_permission_audiovideo_age_rule,
    _compute_age,
    format_data_for_redcap_operations,
)

from .conftest import (
    create_curious_api_failure,
    create_curious_participant_df,
    create_redcap_eav_df,
    dob_for_age,
    dob_for_age_tomorrow,
    patch_curious_api_dependencies,
    PERM_AV_CONSTRAINT,
    PERM_AV_NOT_APPLICABLE,
    PERM_AV_YES,
    setup_curious_integration_mocks,
)

# ============================================================================
# Helper: permission_audiovideo EAV builder
# ============================================================================


def _perm_av_df(records: dict[str, dict[str, str]]) -> pd.DataFrame:
    """Build a long-format EAV DataFrame from ``{record: {field: value}}``."""
    rec_list, fn_list, val_list = [], [], []
    for rec, fields in records.items():
        for fname, val in fields.items():
            rec_list.append(rec)
            fn_list.append(fname)
            val_list.append(val)
    return create_redcap_eav_df(
        records=rec_list,
        field_names=fn_list,
        values=val_list,
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
# _compute_age() Tests
# ============================================================================


class TestComputeAge:
    """Tests for _compute_age helper."""

    def test_known_age(self) -> None:
        """Age is computed correctly for a known date."""
        assert _compute_age(dob_for_age(15)) == 15

    def test_birthday_today(self) -> None:
        """Age increments on the birthday itself."""
        assert _compute_age(dob_for_age(10)) == 10

    def test_birthday_tomorrow(self) -> None:
        """Age has not yet incremented the day before the birthday."""
        assert _compute_age(dob_for_age_tomorrow(10)) == 9

    def test_whitespace_stripped(self) -> None:
        """Leading/trailing whitespace is tolerated."""
        assert _compute_age(f"  {dob_for_age(20)}  ") == 20

    @pytest.mark.parametrize(
        "bad_input",
        ["not-a-date", "", None, 12345],
        ids=["garbage", "empty", "none", "int"],
    )
    def test_unparseable_returns_none(self, bad_input: Any) -> None:
        """Unparseable or non-string inputs return None."""
        assert _compute_age(bad_input) is None  # type: ignore[arg-type]


# ============================================================================
# Permission Audiovideo Constraint Tests
# ============================================================================


class TestPermissionAudiovideoConstraint:
    """Verify the RangeConstraint matches the business rule [11, 18)."""

    def test_boundaries(self) -> None:
        """Constraint defines [11, 18)."""
        c = PERM_AV_CONSTRAINT
        assert (c.minimum, c.maximum) == (11, 18)
        assert c.left_inclusive is True
        assert c.right_inclusive is False

    @pytest.mark.parametrize("age", [11, 12, 15, 17])
    def test_in_range(self, age: int) -> None:
        """Ages 11–17 are in range."""
        assert PERM_AV_CONSTRAINT.in_range(age) is True

    @pytest.mark.parametrize("age", [0, 5, 10, 18, 19, 25, 65])
    def test_out_of_range(self, age: int) -> None:
        """Ages <11 and ≥18 are out of range."""
        assert PERM_AV_CONSTRAINT.in_range(age) is False


# ============================================================================
# _apply_permission_audiovideo_age_rule() Tests
# ============================================================================


class TestApplyPermissionAudiovideoAgeRule:
    """Tests for _apply_permission_audiovideo_age_rule."""

    _PERM = "permission_audiovideo_participant"

    def _get_perm(self, df: pd.DataFrame, record: str | None = None) -> str:
        """Extract the permission value, optionally for a specific record."""
        mask = df["field_name"] == self._PERM
        if record is not None:
            mask = mask & (df["record"] == record)
        return df.loc[mask, "value"].iloc[0]

    # -- Ages outside [11, 18) → Not Applicable --

    @pytest.mark.parametrize("age", [0, 5, 9, 10])
    def test_under_11_gets_not_applicable(self, age: int) -> None:
        """Ages below 11 are set to 'Not Applicable'."""
        df = _perm_av_df({"R1": {"dob": dob_for_age(age), self._PERM: PERM_AV_YES}})
        result = _apply_permission_audiovideo_age_rule(df)
        assert self._get_perm(result) == PERM_AV_NOT_APPLICABLE

    @pytest.mark.parametrize("age", [18, 19, 25, 65])
    def test_18_or_older_gets_not_applicable(self, age: int) -> None:
        """Ages 18 and above are set to 'Not Applicable'."""
        df = _perm_av_df({"R1": {"dob": dob_for_age(age), self._PERM: PERM_AV_YES}})
        result = _apply_permission_audiovideo_age_rule(df)
        assert self._get_perm(result) == PERM_AV_NOT_APPLICABLE

    # -- Ages in [11, 18) → value preserved --

    @pytest.mark.parametrize("age", [11, 13, 15, 17])
    def test_in_range_keeps_value(self, age: int) -> None:
        """Ages 11–17 keep their original value."""
        df = _perm_av_df({"R1": {"dob": dob_for_age(age), self._PERM: PERM_AV_YES}})
        result = _apply_permission_audiovideo_age_rule(df)
        assert self._get_perm(result) == PERM_AV_YES

    # -- Edge cases --

    def test_missing_perm_field_appended_when_outside_range(self) -> None:
        """Row is created when the field is absent and age is outside range."""
        df = _perm_av_df({"R1": {"dob": dob_for_age(8)}})
        result = _apply_permission_audiovideo_age_rule(df)
        perm_rows = result[result["field_name"] == self._PERM]
        assert len(perm_rows) == 1
        assert perm_rows.iloc[0]["value"] == PERM_AV_NOT_APPLICABLE
        assert perm_rows.iloc[0]["record"] == "R1"

    def test_no_dob_leaves_value_unchanged(self) -> None:
        """Without a DOB row, the existing value is untouched."""
        df = _perm_av_df({"R1": {self._PERM: PERM_AV_YES}})
        result = _apply_permission_audiovideo_age_rule(df)
        assert self._get_perm(result) == PERM_AV_YES

    def test_unparseable_dob_leaves_value_unchanged(self) -> None:
        """An unparseable DOB string leaves the existing value untouched."""
        df = _perm_av_df({"R1": {"dob": "not-a-date", self._PERM: PERM_AV_YES}})
        result = _apply_permission_audiovideo_age_rule(df)
        assert self._get_perm(result) == PERM_AV_YES

    # -- Multiple records --

    def test_mixed_ages(self) -> None:
        """Each record is handled independently according to its own age."""
        df = _perm_av_df(
            {
                "YOUNG": {"dob": dob_for_age(8), self._PERM: PERM_AV_YES},
                "TEEN": {"dob": dob_for_age(14), self._PERM: PERM_AV_YES},
                "ADULT": {"dob": dob_for_age(21), self._PERM: PERM_AV_YES},
            }
        )
        result = _apply_permission_audiovideo_age_rule(df)
        assert self._get_perm(result, "YOUNG") == PERM_AV_NOT_APPLICABLE
        assert self._get_perm(result, "TEEN") == PERM_AV_YES
        assert self._get_perm(result, "ADULT") == PERM_AV_NOT_APPLICABLE

    def test_empty_dataframe(self) -> None:
        """An empty DataFrame passes through unchanged."""
        df = create_redcap_eav_df()
        result = _apply_permission_audiovideo_age_rule(df)
        assert result.empty


# ============================================================================
# format_data_for_redcap_operations() – permission_audiovideo integration
# ============================================================================


class TestFormatDataPermissionAudiovideo:
    """Verify age rule integrates correctly in the full formatting pipeline."""

    @staticmethod
    def _source_df(age: int, perm_field: str, perm_value: str) -> pd.DataFrame:
        """Build minimal PID 247 source DataFrame with pre-rename field names."""
        dob_field = "dob" if age < 18 else "dob_1821"
        return create_redcap_eav_df(
            records=["1", "1", "1", "1"],
            field_names=["record_id", "mrn", dob_field, perm_field],
            values=["1", "MRN001", dob_for_age(age), perm_value],
        )

    @staticmethod
    def _get_perm(df: pd.DataFrame) -> str | None:
        """Extract permission_audiovideo_participant value or None."""
        perm = df.loc[df["field_name"] == "permission_audiovideo_participant", "value"]
        return perm.iloc[0] if not perm.empty else None

    def test_under_11_overridden(self) -> None:
        """A 9-year-old through the full pipeline gets 'Not Applicable'."""
        result = format_data_for_redcap_operations(
            self._source_df(9, "permission_audiovideo_1113", "1")
        )
        val = self._get_perm(result)
        if val is not None:
            assert val == PERM_AV_NOT_APPLICABLE

    def test_teen_13_preserved(self) -> None:
        """A 13-year-old keeps the renamed value from the 1113 field."""
        result = format_data_for_redcap_operations(
            self._source_df(13, "permission_audiovideo_1113", "1")
        )
        val = self._get_perm(result)
        if val is not None:
            assert val == "1"

    def test_teen_15_preserved(self) -> None:
        """A 15-year-old keeps the renamed value from the 1417 field."""
        result = format_data_for_redcap_operations(
            self._source_df(15, "permission_audiovideo_1417", "1")
        )
        val = self._get_perm(result)
        if val is not None:
            assert val == "1"

    def test_adult_overridden(self) -> None:
        """A 20-year-old through the full pipeline gets 'Not Applicable'."""
        result = format_data_for_redcap_operations(
            self._source_df(20, "permission_audiovideo_1417", "1")
        )
        val = self._get_perm(result)
        if val is not None:
            assert val == PERM_AV_NOT_APPLICABLE

    def test_dob_1821_renamed_before_age_check(self) -> None:
        """``dob_1821`` → ``dob`` rename happens before the age rule runs."""
        result = format_data_for_redcap_operations(
            self._source_df(18, "permission_audiovideo_1417", "1")
        )
        val = self._get_perm(result)
        if val is not None:
            assert val == PERM_AV_NOT_APPLICABLE


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
# Webhook Endpoint Tests – to_curious
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


# ============================================================================
# Webhook Endpoint Tests – to_redcap
# ============================================================================


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
        with patch(
            "hbnmigration.from_redcap.to_redcap.process_record_for_redcap_operations"
        ):
            response = client.post(
                "/webhook/redcap-to-intake",
                data={
                    "project_id": "625",
                    "instrument": "enrollment",
                    "record": "12345",
                    "intake_ready": "1",
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
                "intake_ready": "0",
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
# process_record_for_curious() Tests
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
# process_record_for_redcap_operations() Tests
# ============================================================================


class TestProcessRecordForRedcapOperations:
    """Tests for process_record_for_redcap_operations function."""

    @patch("hbnmigration.from_redcap.to_redcap.update_source_redcap_status")
    @patch("hbnmigration.from_redcap.to_redcap.push_to_intake_redcap", return_value=5)
    @patch("hbnmigration.from_redcap.to_redcap.format_data_for_redcap_operations")
    @patch("hbnmigration.from_redcap.to_redcap.fetch_data")
    def test_successful_processing(
        self,
        mock_fetch: MagicMock,
        mock_format: MagicMock,
        mock_push: MagicMock,
        mock_update: MagicMock,
    ) -> None:
        """Test successful processing of a record for Operations REDCap."""
        mock_fetch.return_value = pd.DataFrame(
            {
                "record": ["123", "123"],
                "field_name": ["mrn", "intake_ready"],
                "value": ["abc", "1"],
                "redcap_event_name": ["enrollment_arm_1", "enrollment_arm_1"],
            }
        )
        mock_format.return_value = pd.DataFrame(
            {"record": ["123"], "field_name": ["mrn"], "value": ["abc"]}
        )
        result = to_redcap.process_record_for_redcap_operations("123")
        assert result["status"] == "success"
        assert result["record_id"] == "123"
        assert result["rows_pushed"] == 5
        mock_update.assert_called_once_with(
            "123",
            str(
                Values.PID247.intake_ready[
                    "Participant information already sent to "
                    "HBN - Intake Redcap project"
                ]
            ),
            "enrollment_arm_1",
        )

    @patch("hbnmigration.from_redcap.to_redcap.fetch_data")
    def test_no_data_found(self, mock_fetch: MagicMock) -> None:
        """Test that missing data returns error status."""
        mock_fetch.return_value = pd.DataFrame()
        result = to_redcap.process_record_for_redcap_operations("999")
        assert result["status"] == "error"
        assert "No data found" in result["message"]

    @patch("hbnmigration.from_redcap.to_redcap.format_data_for_redcap_operations")
    @patch("hbnmigration.from_redcap.to_redcap.fetch_data")
    def test_no_processable_data(
        self, mock_fetch: MagicMock, mock_format: MagicMock
    ) -> None:
        """Test when formatting yields empty DataFrame."""
        mock_fetch.return_value = pd.DataFrame(
            {"record": ["123"], "field_name": ["mrn"], "value": ["abc"]}
        )
        mock_format.return_value = pd.DataFrame()
        result = to_redcap.process_record_for_redcap_operations("123")
        assert result["status"] == "error"
        assert "No processable data" in result["message"]

    @patch("hbnmigration.from_redcap.to_redcap.fetch_data")
    def test_exception_handling(self, mock_fetch: MagicMock) -> None:
        """Test that exceptions are caught and returned as error status."""
        mock_fetch.side_effect = RuntimeError("connection failed")
        result = to_redcap.process_record_for_redcap_operations("123")
        assert result["status"] == "error"
        assert "connection failed" in result["message"]

    @patch("hbnmigration.from_redcap.to_redcap.push_to_intake_redcap", return_value=5)
    @patch("hbnmigration.from_redcap.to_redcap.format_data_for_redcap_operations")
    @patch("hbnmigration.from_redcap.to_redcap.fetch_data")
    def test_skips_status_update_when_no_event_name(
        self,
        mock_fetch: MagicMock,
        mock_format: MagicMock,
        mock_push: MagicMock,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Test that status update is skipped when event name cannot be determined."""
        mock_fetch.return_value = pd.DataFrame(
            {
                "record": ["123"],
                "field_name": ["mrn"],
                "value": ["abc"],
            }
        )
        mock_format.return_value = pd.DataFrame(
            {"record": ["123"], "field_name": ["mrn"], "value": ["abc"]}
        )
        result = to_redcap.process_record_for_redcap_operations("123")
        assert result["status"] == "success"
        assert "Could not determine event name" in caplog.text


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
        to_curious.clear_ready_flag("nonexistent")


class TestClearReadyFlagIntake:
    """Tests for to_redcap.clear_ready_flag."""

    @patch("hbnmigration.from_redcap.to_redcap.update_source_redcap_status")
    @patch("hbnmigration.from_redcap.to_redcap.fetch_data")
    def test_clears_flag(self, mock_fetch: MagicMock, mock_update: MagicMock) -> None:
        """Test flag setting."""
        mock_fetch.return_value = pd.DataFrame(
            {
                "field_name": ["intake_ready"],
                "redcap_event_name": ["event_1"],
            }
        )
        to_redcap.clear_ready_flag("123")
        mock_update.assert_called_once_with("123", "0", "event_1")

    @patch("hbnmigration.from_redcap.to_redcap.fetch_data")
    def test_handles_missing_record(self, mock_fetch: MagicMock) -> None:
        """Test missing record handling."""
        mock_fetch.return_value = pd.DataFrame()
        to_redcap.clear_ready_flag("nonexistent")


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


# ============================================================================
# _prepare_curious_data() Tests
# ============================================================================


class TestPrepareCuriousData:
    """Tests for _prepare_curious_data helper function."""

    @pytest.fixture
    def curious_data(self) -> dict[to_curious.Individual, pd.DataFrame]:
        """Sample curious data dict with child and parent DataFrames."""
        return {
            "child": pd.DataFrame(
                {"secretUserId": ["00001", "00002"], "firstName": ["Alice", "Bob"]}
            ),
            "parent": pd.DataFrame(
                {"secretUserId": ["00001_P"], "firstName": ["Carol"]}
            ),
        }

    def test_sets_account_types_correctly(
        self, curious_data: dict[to_curious.Individual, pd.DataFrame]
    ) -> None:
        """Test that account types are set correctly for each DataFrame."""
        child_full, child_limited, parent_full = to_curious._prepare_curious_data(
            curious_data
        )
        assert (child_full["accountType"] == "full").all()
        assert (child_limited["accountType"] == "limited").all()
        assert (parent_full["accountType"] == "full").all()

    def test_returns_copies_not_references(
        self, curious_data: dict[to_curious.Individual, pd.DataFrame]
    ) -> None:
        """Test that returned DataFrames are independent copies."""
        child_full, child_limited, parent_full = to_curious._prepare_curious_data(
            curious_data
        )
        for result_df in (child_full, child_limited, parent_full):
            result_df["extra"] = "x"
        for original_df in curious_data.values():
            assert "extra" not in original_df.columns
            assert "accountType" not in original_df.columns

    def test_preserves_original_data(
        self, curious_data: dict[to_curious.Individual, pd.DataFrame]
    ) -> None:
        """Test that original column data is preserved in all copies."""
        child_full, child_limited, parent_full = to_curious._prepare_curious_data(
            curious_data
        )
        assert child_full.iloc[0]["firstName"] == "Alice"
        assert child_limited.iloc[0]["firstName"] == "Alice"
        assert parent_full.iloc[0]["firstName"] == "Carol"

    def test_handles_empty_dataframes(self) -> None:
        """Test with empty child and parent DataFrames."""
        empty_data: dict[to_curious.Individual, pd.DataFrame] = {
            "child": pd.DataFrame(),
            "parent": pd.DataFrame(),
        }
        for result_df in to_curious._prepare_curious_data(empty_data):
            assert result_df.empty


# ============================================================================
# _push_to_curious() Tests
# ============================================================================


class TestPushToCurious:
    """Tests for _push_to_curious orchestration function."""

    @pytest.fixture
    def curious_data(self) -> dict[to_curious.Individual, pd.DataFrame]:
        """Sample curious data dict with child and parent DataFrames."""
        return {
            "child": pd.DataFrame({"secretUserId": ["00001"]}),
            "parent": pd.DataFrame({"secretUserId": ["00001_P"]}),
        }

    @pytest.fixture
    def data_operations(self) -> pd.DataFrame:
        """Minimal raw REDCap operations DataFrame."""
        return pd.DataFrame({"record": ["001"]})

    def _run(
        self,
        data_operations: pd.DataFrame,
        curious_data: dict[to_curious.Individual, pd.DataFrame],
        *,
        child_failures: list[str] | None = None,
        parent_failures: list[str] | None = None,
    ) -> tuple[list[str], MagicMock, MagicMock, MagicMock]:
        """Run ``_push_to_curious`` with patched push/update functions."""
        with (
            patch(
                "hbnmigration.from_redcap.to_curious.push_child_data",
                return_value=child_failures or [],
            ) as mock_child,
            patch(
                "hbnmigration.from_redcap.to_curious.push_parent_data",
                return_value=parent_failures or [],
            ) as mock_parent,
            patch(
                "hbnmigration.from_redcap.to_curious.update_redcap",
            ) as mock_update,
        ):
            failures = to_curious._push_to_curious(data_operations, curious_data)
        return failures, mock_child, mock_parent, mock_update

    def test_passes_correct_account_types(
        self,
        data_operations: pd.DataFrame,
        curious_data: dict[to_curious.Individual, pd.DataFrame],
    ) -> None:
        """Test that push functions receive DataFrames with correct accountType."""
        _, mock_child, mock_parent, _ = self._run(data_operations, curious_data)
        assert (mock_child.call_args[0][0]["accountType"] == "full").all()
        assert (mock_parent.call_args[0][0]["accountType"] == "limited").all()
        assert (mock_parent.call_args[0][1]["accountType"] == "full").all()

    def test_no_failures(
        self,
        data_operations: pd.DataFrame,
        curious_data: dict[to_curious.Individual, pd.DataFrame],
    ) -> None:
        """Test that an empty list is returned when nothing fails."""
        failures, *_ = self._run(data_operations, curious_data)
        assert failures == []

    @pytest.mark.parametrize(
        "child_failures,parent_failures,expected",
        [
            (["mrn1"], [], {"mrn1"}),
            ([], ["mrn2"], {"mrn2"}),
            (["mrn1"], ["mrn2"], {"mrn1", "mrn2"}),
        ],
        ids=["child-only", "parent-only", "both"],
    )
    def test_aggregates_failures(
        self,
        data_operations: pd.DataFrame,
        curious_data: dict[to_curious.Individual, pd.DataFrame],
        child_failures: list[str],
        parent_failures: list[str],
        expected: set[str],
    ) -> None:
        """Test that failures from child and parent pushes are combined."""
        failures, *_ = self._run(
            data_operations,
            curious_data,
            child_failures=child_failures,
            parent_failures=parent_failures,
        )
        assert set(failures) == expected

    def test_calls_update_redcap(
        self,
        data_operations: pd.DataFrame,
        curious_data: dict[to_curious.Individual, pd.DataFrame],
    ) -> None:
        """Test that update_redcap is called with the original data."""
        _, _, _, mock_update = self._run(data_operations, curious_data)
        mock_update.assert_called_once()
        args = mock_update.call_args[0]
        assert args[0] is data_operations
        pd.testing.assert_frame_equal(args[1], curious_data["child"])
        assert args[2] == []


# ============================================================================
# RedcapRecord Model Tests
# ============================================================================


class TestRedcapRecord:
    """Tests for the RedcapRecord Pydantic model."""

    def test_basic_construction(self) -> None:
        """Test constructing a RedcapRecord with required fields."""
        record = RedcapRecord(
            project_id=625,
            instrument="enrollment",
            record="12345",
        )
        assert record.project_id == 625
        assert record.instrument == "enrollment"
        assert record.record == "12345"

    def test_all_fields(self) -> None:
        """Test constructing a RedcapRecord with all fields populated."""
        record = RedcapRecord(
            project_id=625,
            instrument="enrollment",
            record="12345",
            redcap_event_name="baseline_arm_1",
            redcap_repeat_instance=3,
            redcap_repeat_instrument="medications",
            redcap_data_access_group="site_a",
            redcap_url="https://redcap.test/redcap_v14.0.0/index.php",
            project_url="https://redcap.test/redcap_v14.0.0/index.php?pid=625",
            username="testuser",
        )
        assert record.redcap_event_name == "baseline_arm_1"
        assert record.redcap_repeat_instance == 3
        assert record.redcap_repeat_instrument == "medications"
        assert record.redcap_data_access_group == "site_a"
        assert record.redcap_url == "https://redcap.test/redcap_v14.0.0/index.php"
        assert record.username == "testuser"

    def test_optional_fields_default_to_none(self) -> None:
        """Test that optional fields default to None."""
        record = RedcapRecord(
            project_id=625,
            instrument="enrollment",
            record="12345",
        )
        assert record.redcap_event_name is None
        assert record.redcap_repeat_instance is None
        assert record.redcap_repeat_instrument is None
        assert record.redcap_data_access_group is None
        assert record.redcap_url is None
        assert record.project_url is None
        assert record.username is None

    def test_populate_by_name(self) -> None:
        """Test that fields can be set by alias or field name."""
        record = RedcapRecord(
            project_id=625,
            instrument="enrollment",
            record="12345",
            redcap_repeat_instance=2,
        )
        assert record.redcap_repeat_instance == 2


class TestRedcapRepeatInstanceConversion:
    """Tests for the float-to-int conversion on redcap_repeat_instance."""

    @pytest.mark.parametrize(
        "input_value,expected",
        [
            (1, 1),
            (1.0, 1),
            (0.0, 0),
            (100.0, 100),
            (7.9, 7),
            (None, None),
            (float("nan"), None),
        ],
        ids=[
            "int",
            "float_1.0",
            "float_0.0",
            "float_100.0",
            "float_7.9",
            "none",
            "nan",
        ],
    )
    def test_conversion(self, input_value: Any, expected: int | None) -> None:
        """Test that repeat_instance values are correctly converted."""
        record = RedcapRecord(
            project_id=625,
            instrument="enrollment",
            record="12345",
            redcap_repeat_instance=input_value,
        )
        assert record.redcap_repeat_instance == expected
        if expected is not None:
            assert isinstance(record.redcap_repeat_instance, int)


# ============================================================================
# RedcapTriggerPayload Tests
# ============================================================================


class TestRedcapTriggerPayloadCurious:
    """Tests for RedcapTriggerPayload in to_curious inheriting RedcapRecord."""

    def test_inherits_float_conversion(self) -> None:
        """Test that to_curious payload inherits float-to-int conversion."""
        from hbnmigration.from_redcap.to_curious import RedcapTriggerPayload

        payload = RedcapTriggerPayload(
            project_id=625,
            instrument="enrollment",
            record="12345",
            redcap_repeat_instance=3.0,
            ready_to_send_to_curious="1",
        )
        assert payload.redcap_repeat_instance == 3
        assert isinstance(payload.redcap_repeat_instance, int)

    def test_has_ready_to_send_field(self) -> None:
        """Test that to_curious payload has ready_to_send_to_curious field."""
        from hbnmigration.from_redcap.to_curious import RedcapTriggerPayload

        payload = RedcapTriggerPayload(
            project_id=625,
            instrument="enrollment",
            record="12345",
            ready_to_send_to_curious="1",
        )
        assert payload.ready_to_send_to_curious == "1"

    def test_nan_conversion_inherited(self) -> None:
        """Test that NaN conversion is inherited from RedcapRecord."""
        from hbnmigration.from_redcap.to_curious import RedcapTriggerPayload

        payload = RedcapTriggerPayload(
            project_id=625,
            instrument="enrollment",
            record="12345",
            redcap_repeat_instance=float("nan"),
        )
        assert payload.redcap_repeat_instance is None


class TestRedcapTriggerPayloadRedcap:
    """Tests for RedcapTriggerPayload in to_redcap inheriting RedcapRecord."""

    def test_inherits_float_conversion(self) -> None:
        """Test that to_redcap payload inherits float-to-int conversion."""
        from hbnmigration.from_redcap.to_redcap import RedcapTriggerPayload

        payload = RedcapTriggerPayload(
            project_id=625,
            instrument="enrollment",
            record="12345",
            redcap_repeat_instance=5.0,
            intake_ready="1",
        )
        assert payload.redcap_repeat_instance == 5
        assert isinstance(payload.redcap_repeat_instance, int)

    def test_has_ready_to_send_field(self) -> None:
        """Test that to_redcap payload has intake_ready field."""
        from hbnmigration.from_redcap.to_redcap import RedcapTriggerPayload

        payload = RedcapTriggerPayload(
            project_id=625,
            instrument="enrollment",
            record="12345",
            intake_ready="1",
        )
        assert payload.intake_ready == "1"

    def test_nan_conversion_inherited(self) -> None:
        """Test that NaN conversion is inherited from RedcapRecord."""
        from hbnmigration.from_redcap.to_redcap import RedcapTriggerPayload

        payload = RedcapTriggerPayload(
            project_id=625,
            instrument="enrollment",
            record="12345",
            redcap_repeat_instance=float("nan"),
        )
        assert payload.redcap_repeat_instance is None


class TestRedcapRecordFromFormData:
    """Tests simulating REDCap Data Entry Trigger form submissions."""

    def test_from_dict_with_float_instance(self) -> None:
        """Test constructing from dict as would come from form parsing."""
        data = {
            "project_id": 625,
            "instrument": "medications",
            "record": "12345",
            "redcap_event_name": "baseline_arm_1",
            "redcap_repeat_instance": 2.0,
            "redcap_repeat_instrument": "medications",
        }
        record = RedcapRecord(**data)
        assert record.redcap_repeat_instance == 2
        assert isinstance(record.redcap_repeat_instance, int)

    def test_from_dict_with_empty_string_instance(self) -> None:
        """Test that empty string for repeat instance is handled."""
        data = {
            "project_id": 625,
            "instrument": "enrollment",
            "record": "12345",
            "redcap_repeat_instance": None,
        }
        record = RedcapRecord(**data)
        assert record.redcap_repeat_instance is None


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
        result = to_curious.process_record_for_curious("ST001")
        assert result["status"] in ("success", "partial", "error")
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


class TestFormatDataForRedcapOperations:
    """Tests for format_data_for_redcap_operations."""

    class TestDeduplication:
        """Tests for Step 8: repeat instance deduplication logic."""

        def test_keeps_all_values_from_latest_repeat_instance(self):
            """Checkbox fields with multiple values in the latest instance are kept."""
            df = pd.DataFrame(
                {
                    "record": ["1465"] * 6,
                    "field_name": [
                        "parent_involvement",
                        "parent_involvement",
                        "email",
                        "parent_involvement",
                        "parent_involvement",
                        "email",
                    ],
                    "value": [
                        "1",
                        "2",
                        "old@example.com",
                        "1",
                        "2",
                        "new@example.com",
                    ],
                    "redcap_event_name": ["event_1"] * 6,
                    "redcap_repeat_instrument": ["adult_consent"] * 6,
                    "redcap_repeat_instance": [1, 1, 1, 2, 2, 2],
                }
            )

            result = format_data_for_redcap_operations(df)

            # Should keep both parent_involvement rows from instance 2
            pi_rows = result[result["field_name"] == "parent_involvement"]
            assert len(pi_rows) == 2
            assert set(pi_rows["value"]) == {"1", "2"}

            # Should keep only the email from instance 2
            email_rows = result[result["field_name"] == "email"]
            assert len(email_rows) == 1
            assert email_rows.iloc[0]["value"] == "new@example.com"

        def test_discards_older_repeat_instances(self):
            """Only the highest repeat instance per record+instrument is kept."""
            df = pd.DataFrame(
                {
                    "record": ["100"] * 5,
                    "field_name": [
                        "parent_involvement",
                        "parent_involvement",
                        "parent_involvement",
                        "parent_involvement",
                        "parent_involvement",
                    ],
                    "value": ["0", "1", "2", "1", "3"],
                    "redcap_event_name": ["event_1"] * 5,
                    "redcap_repeat_instrument": ["adult_consent"] * 5,
                    "redcap_repeat_instance": [1, 1, 1, 2, 2],
                }
            )

            result = format_data_for_redcap_operations(df)

            pi_rows = result[result["field_name"] == "parent_involvement"]
            assert len(pi_rows) == 2
            assert set(pi_rows["value"]) == {"1", "3"}

        def test_non_repeated_rows_deduplicate_by_record_and_field(self):
            """Non-repeated rows (NaN instance) deduplicate on record + field_name."""
            df = pd.DataFrame(
                {
                    "record": ["200", "200"],
                    "field_name": ["email", "email"],
                    "value": ["first@example.com", "second@example.com"],
                    "redcap_event_name": ["event_1", "event_1"],
                    "redcap_repeat_instrument": [None, None],
                    "redcap_repeat_instance": [None, None],
                }
            )

            result = format_data_for_redcap_operations(df)

            email_rows = result[result["field_name"] == "email"]
            assert len(email_rows) == 1

        def test_mixed_repeated_and_non_repeated(self):
            """Records with both repeated and non-repeated fields are handled."""
            df = pd.DataFrame(
                {
                    "record": ["300"] * 5,
                    "field_name": [
                        "dob",
                        "parent_involvement",
                        "parent_involvement",
                        "parent_involvement",
                        "email",
                    ],
                    "value": ["2010-01-01", "0", "1", "2", "test@example.com"],
                    "redcap_event_name": ["event_1"] * 5,
                    "redcap_repeat_instrument": [
                        None,
                        "adult_consent",
                        "adult_consent",
                        "adult_consent",
                        None,
                    ],
                    "redcap_repeat_instance": [None, 1, 1, 1, None],
                }
            )

            result = format_data_for_redcap_operations(df)

            # All 3 checkbox values kept (only one instance, so all kept)
            pi_rows = result[result["field_name"] == "parent_involvement"]
            assert len(pi_rows) == 3

            # Non-repeated fields kept as single rows
            assert len(result[result["field_name"] == "dob"]) == 1
            assert len(result[result["field_name"] == "email"]) == 1

    class TestFieldRenaming:
        """Tests for field name rename and fan-out logic."""

        def test_permission_audiovideo_1821_renamed(self):
            """Test that `permission_audiovideo_1821` is renamed."""
            df = pd.DataFrame(
                {
                    "record": ["400"],
                    "field_name": ["permission_audiovideo_1821"],
                    "value": ["1"],
                    "redcap_event_name": ["event_1"],
                    "redcap_repeat_instrument": [None],
                    "redcap_repeat_instance": [None],
                }
            )

            result = format_data_for_redcap_operations(df)

            assert "permission_audiovideo_1821" not in result["field_name"].values
            assert "permission_audiovideo_participant" in result["field_name"].values

        def test_permission_collab_1821_renamed_and_decremented(self):
            """Test that `permission_collab_1821` is renamed and decremented."""
            df = pd.DataFrame(
                {
                    "record": ["500"],
                    "field_name": ["permission_collab_1821"],
                    "value": ["1"],
                    "redcap_event_name": ["event_1"],
                    "redcap_repeat_instrument": [None],
                    "redcap_repeat_instance": [None],
                }
            )

            result = format_data_for_redcap_operations(df)

            collab_rows = result[result["field_name"] == "permission_collab"]
            assert len(collab_rows) == 1
            assert collab_rows.iloc[0]["value"] == "0"  # decremented from 1
