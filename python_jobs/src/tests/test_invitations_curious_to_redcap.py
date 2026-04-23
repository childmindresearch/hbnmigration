"""Tests for invitations_to_redcap module."""

from typing import Any, cast
from unittest.mock import Mock, patch

import pandas as pd
import polars as pl
import pytest
import requests

from hbnmigration.from_curious.invitations_to_redcap import (
    check_activity_response,
    check_activity_responses,
    create_invitation_record,
    format_for_redcap,
    main,
    pull_data_from_curious,
    push_to_redcap,
    update_already_completed,
)
from hbnmigration.utility_functions import CuriousDecryptedAnswer
from hbnmigration.utility_functions.custom import fetch_api_data

from .conftest import (
    INVITATIONS_MOD,
    make_api_respondent,
    make_ml_data,
    patch_invitations_module,
    SAMPLE_ACTIVITY_ID,
    SAMPLE_APPLET_ID,
    SAMPLE_SUBJECT_ID,
)

# ============================================================================
# Helpers
# ============================================================================


def _mock_http_response(
    status_code: int = requests.codes["okay"],
    json_data: Any = None,
    *,
    raise_for_status: Exception | None = None,
    reason: str = "",
    text: str = "",
) -> Mock:
    """
    Build a mock ``requests.Response``.

    Parameters
    ----------
    status_code
        HTTP status code.
    json_data
        Return value for ``response.json()``.
    raise_for_status
        If set, ``response.raise_for_status()`` raises this.
    reason
        Value for ``response.reason``.
    text
        Value for ``response.text``.

    """
    resp = Mock()
    resp.status_code = status_code
    resp.reason = reason
    resp.text = text
    if json_data is not None:
        resp.json.return_value = json_data
    if raise_for_status:
        resp.raise_for_status.side_effect = raise_for_status
    else:
        resp.raise_for_status = Mock()
    return resp


def _single_record_df() -> pl.DataFrame:
    """Return a one-row invitation DataFrame for main() tests."""
    return pl.DataFrame(
        {
            "record_id": ["00001"],
            "source_secret_id": ["00001_P"],
            "invite_status": [3],
            "redcap_event_name": ["curious_parent_arm_1"],
            "complete": ["0"],
            "respondent_id": [SAMPLE_SUBJECT_ID],
        }
    )


def _empty_invitation_schema() -> pl.DataFrame:
    """Return an empty DataFrame with the invitation schema."""
    return pl.DataFrame(
        schema={
            "record_id": pl.String,
            "source_secret_id": pl.String,
            "invite_status": pl.Int64,
            "redcap_event_name": pl.String,
            "complete": pl.String,
            "respondent_id": pl.String,
        }
    )


# ============================================================================
# Tests: create_invitation_record
# ============================================================================


class TestCreateInvitationRecord:
    """Tests for create_invitation_record."""

    def test_returns_dict_for_valid_parent(
        self, sample_respondent: dict[str, Any]
    ) -> None:
        """Verify all expected keys are present and correct."""
        result = create_invitation_record(sample_respondent, SAMPLE_APPLET_ID)
        assert result is not None
        assert result["record_id"] == "00001"
        assert result["source_secret_id"] == "00001_P"
        assert result["invite_status"] == 3
        assert result["redcap_event_name"] == "curious_parent_arm_1"
        assert result["complete"] == "0"
        assert result["respondent_id"] == SAMPLE_SUBJECT_ID

    def test_returns_none_when_no_matching_applet(
        self, sample_respondent: dict[str, Any]
    ) -> None:
        """Non-matching applet ID should yield None."""
        assert (
            create_invitation_record(
                sample_respondent, "wrong_id-ab12-cd34-ef56-abcdef123456"
            )
            is None
        )

    def test_returns_none_when_no_parent_suffix(
        self, sample_respondent: dict[str, Any]
    ) -> None:
        """Secret IDs without ``_P`` suffix are not parent records."""
        sample_respondent["details"][0]["respondentSecretId"] = "00001"
        assert create_invitation_record(sample_respondent, SAMPLE_APPLET_ID) is None

    def test_handles_numeric_secret_id(self, sample_respondent: dict[str, Any]) -> None:
        """Numeric secret IDs should be converted to string record IDs."""
        sample_respondent["details"][0]["respondentSecretId"] = "12345_P"
        result = create_invitation_record(sample_respondent, SAMPLE_APPLET_ID)
        assert result is not None
        assert result["record_id"] == "12345"

    def test_handles_non_numeric_secret_id(
        self, sample_respondent: dict[str, Any]
    ) -> None:
        """Non-numeric secret IDs should pass through as-is."""
        sample_respondent["details"][0]["respondentSecretId"] = "ABC_P"
        result = create_invitation_record(sample_respondent, SAMPLE_APPLET_ID)
        assert result is not None
        assert result["record_id"] == "ABC"
        assert result["source_secret_id"] == "ABC_P"

    def test_uses_last_detail_when_multiple(
        self, sample_respondent: dict[str, Any]
    ) -> None:
        """When multiple details match, use the last one."""
        sample_respondent["details"].append(
            {
                "appletId": SAMPLE_APPLET_ID,
                "respondentSecretId": "99999_P",
                "subjectId": "subj9999-ab12-cd34-ef56-abcdef123456",
            }
        )
        result = create_invitation_record(sample_respondent, SAMPLE_APPLET_ID)
        assert result is not None
        assert result["record_id"] == "99999"

    @pytest.mark.parametrize(
        "status,expected",
        [("not_invited", 1), ("pending", 2), ("invited", 3)],
    )
    def test_maps_invitation_statuses(
        self, sample_respondent: dict[str, Any], status: str, expected: int
    ) -> None:
        """Each Curious status maps to the correct numeric value."""
        sample_respondent["status"] = status
        result = create_invitation_record(sample_respondent, SAMPLE_APPLET_ID)
        assert result is not None
        assert result["invite_status"] == expected

    def test_empty_details_returns_none(
        self, sample_respondent: dict[str, Any]
    ) -> None:
        """Respondent with no details should return None."""
        sample_respondent["details"] = []
        assert create_invitation_record(sample_respondent, SAMPLE_APPLET_ID) is None


# ============================================================================
# Tests: update_already_completed
# ============================================================================


class TestUpdateAlreadyCompleted:
    """Tests for update_already_completed."""

    @staticmethod
    def _run(df: pl.DataFrame, already_completed: list[str]) -> pl.DataFrame:
        """Run update_already_completed with mocked fetch_api_data."""
        with patch_invitations_module() as mocks:
            mocks["fetch_api_data"].return_value = already_completed
            return update_already_completed(df)

    def test_filters_out_completed_records(
        self, sample_invitation_df: pl.DataFrame
    ) -> None:
        """Records already complete in REDCap should be filtered out."""
        result = self._run(sample_invitation_df, ["00001"])
        assert "00001" not in result["record_id"].to_list()
        assert "00002" in result["record_id"].to_list()

    def test_keeps_all_when_none_completed(
        self, sample_invitation_df: pl.DataFrame
    ) -> None:
        """All records kept when none are already complete."""
        assert self._run(sample_invitation_df, []).shape[0] == 2

    def test_filters_all_when_all_completed(
        self, sample_invitation_df: pl.DataFrame
    ) -> None:
        """Empty result when all records are already complete."""
        assert self._run(sample_invitation_df, ["00001", "00002"]).is_empty()

    def test_drops_nulls(self) -> None:
        """Rows with null values should be dropped."""
        df = pl.DataFrame(
            {
                "record_id": ["00001", None, "00003"],
                "source_secret_id": ["00001_P", None, "00003_P"],
                "invite_status": [3, None, 2],
                "redcap_event_name": [
                    "curious_parent_arm_1",
                    None,
                    "curious_parent_arm_1",
                ],
                "complete": ["0", None, "0"],
                "respondent_id": [SAMPLE_SUBJECT_ID, None, SAMPLE_SUBJECT_ID],
            }
        )
        assert self._run(df, []).shape[0] == 2


# ============================================================================
# Tests: check_activity_response
# ============================================================================


class TestCheckActivityResponse:
    """Tests for check_activity_response."""

    @staticmethod
    def _call(
        context: dict[str, Any],
        api_response: Mock,
    ) -> list[Any]:
        """Call check_activity_response with standard mocks."""
        with patch_invitations_module() as mocks:
            mocks["requests_get"].return_value = api_response
            mocks["get_applet_encryption"].return_value = {}
            return check_activity_response(
                "token", context, SAMPLE_APPLET_ID, SAMPLE_ACTIVITY_ID
            )

    def test_returns_empty_list_on_non_ok_response(
        self, sample_redcap_context: dict[str, Any]
    ) -> None:
        """Non-200 status code yields empty list."""
        assert self._call(sample_redcap_context, _mock_http_response(404)) == []

    def test_returns_empty_list_when_no_results(
        self, sample_redcap_context: dict[str, Any]
    ) -> None:
        """Empty result list from API yields empty list."""
        resp = _mock_http_response(json_data={"result": []})
        assert self._call(sample_redcap_context, resp) == []

    def test_returns_empty_list_when_result_is_none(
        self, sample_redcap_context: dict[str, Any]
    ) -> None:
        """None result from API yields empty list."""
        resp = _mock_http_response(json_data={"result": None})
        assert self._call(sample_redcap_context, resp) == []

    def test_uses_target_subject_id_in_request(
        self, sample_redcap_context: dict[str, Any]
    ) -> None:
        """Request URL should contain targetSubjectId parameter."""
        with patch_invitations_module() as mocks:
            mocks["requests_get"].return_value = _mock_http_response(404)
            mocks["get_applet_encryption"].return_value = {}
            check_activity_response(
                "token", sample_redcap_context, SAMPLE_APPLET_ID, SAMPLE_ACTIVITY_ID
            )
            call_url = mocks["requests_get"].call_args[0][0]
            assert (
                f"targetSubjectId={sample_redcap_context['respondent_id']}" in call_url
            )


# ============================================================================
# Tests: check_activity_responses
# ============================================================================


class TestCheckActivityResponses:
    """Tests for check_activity_responses."""

    @staticmethod
    def _call(
        df: pl.DataFrame, response_return: list[Any] | None = None
    ) -> pl.DataFrame:
        """Call check_activity_responses with a mocked single-response handler."""
        with patch(
            f"{INVITATIONS_MOD}.check_activity_response",
            return_value=response_return or [],
        ):
            return check_activity_responses(
                "token", df, SAMPLE_APPLET_ID, SAMPLE_ACTIVITY_ID
            )

    def test_returns_original_df_when_no_responses(
        self, sample_invitation_df: pl.DataFrame
    ) -> None:
        """Original DataFrame returned when no activity responses found."""
        assert self._call(sample_invitation_df).shape == sample_invitation_df.shape

    def test_iterates_over_all_rows(self, sample_invitation_df: pl.DataFrame) -> None:
        """check_activity_response called once per row."""
        with patch(
            f"{INVITATIONS_MOD}.check_activity_response", return_value=[]
        ) as mock_check:
            check_activity_responses(
                "token", sample_invitation_df, SAMPLE_APPLET_ID, SAMPLE_ACTIVITY_ID
            )
            assert mock_check.call_count == sample_invitation_df.shape[0]

    def test_concatenates_responses(self, sample_invitation_df: pl.DataFrame) -> None:
        """Output DataFrames from responses are concatenated."""
        named_output = Mock()
        named_output.output = pl.DataFrame({"record_id": ["00001"], "value": ["test"]})
        with patch(
            f"{INVITATIONS_MOD}.check_activity_response", return_value=[named_output]
        ):
            result = check_activity_responses(
                "token", sample_invitation_df, SAMPLE_APPLET_ID, SAMPLE_ACTIVITY_ID
            )
            assert result.shape[0] == 2

    def test_handles_empty_df(self) -> None:
        """Empty input DataFrame produces empty output."""
        assert self._call(_empty_invitation_schema()).is_empty()


# ============================================================================
# Tests: format_for_redcap
# ============================================================================


class TestFormatForRedcap:
    """Tests for format_for_redcap."""

    def test_returns_empty_list_for_empty_data(
        self, sample_redcap_context: dict[str, Any]
    ) -> None:
        """Empty dict input returns empty list."""
        assert (
            format_for_redcap(cast(CuriousDecryptedAnswer, {}), sample_redcap_context)
            == []
        )

    def test_returns_empty_list_for_none_data(
        self, sample_redcap_context: dict[str, Any]
    ) -> None:
        """None input returns empty list."""
        assert (
            format_for_redcap(cast(CuriousDecryptedAnswer, None), sample_redcap_context)
            == []
        )

    def test_produces_named_outputs(
        self,
        sample_decrypted_answer: CuriousDecryptedAnswer,
        sample_redcap_context: dict[str, Any],
    ) -> None:
        """Valid input produces NamedOutput objects with Polars DataFrames."""
        results = format_for_redcap(sample_decrypted_answer, sample_redcap_context)
        assert len(results) > 0
        for result in results:
            assert hasattr(result, "name")
            assert isinstance(result.output, pl.DataFrame)

    def test_output_contains_record_id(
        self,
        sample_decrypted_answer: CuriousDecryptedAnswer,
        sample_redcap_context: dict[str, Any],
    ) -> None:
        """Output DataFrame includes record_id from context."""
        for result in format_for_redcap(sample_decrypted_answer, sample_redcap_context):
            assert result.output["record_id"][0] == "00001"

    def test_output_contains_redcap_event_name(
        self,
        sample_decrypted_answer: CuriousDecryptedAnswer,
        sample_redcap_context: dict[str, Any],
    ) -> None:
        """Output DataFrame includes redcap_event_name from context."""
        for result in format_for_redcap(sample_decrypted_answer, sample_redcap_context):
            assert result.output["redcap_event_name"][0] == "curious_parent_arm_1"

    def test_output_contains_context_fields(
        self,
        sample_decrypted_answer: CuriousDecryptedAnswer,
        sample_redcap_context: dict[str, Any],
    ) -> None:
        """Output DataFrame includes mapped context fields."""
        expected_cols = {
            "curious_account_created_source_secret_id",
            "curious_account_created_invite_status",
            "curious_account_created_complete",
        }
        for result in format_for_redcap(sample_decrypted_answer, sample_redcap_context):
            assert expected_cols.issubset(result.output.columns)

    def test_column_names_normalized(
        self,
        sample_decrypted_answer: CuriousDecryptedAnswer,
        sample_redcap_context: dict[str, Any],
    ) -> None:
        """Column names should use curious_account_created_ prefix."""
        for result in format_for_redcap(sample_decrypted_answer, sample_redcap_context):
            bad = [
                c
                for c in result.output.columns
                if c.startswith("curiousaccountcreated_")
            ]
            assert bad == [], f"Columns not renamed: {bad}"

    def test_handles_missing_items_raises(
        self, sample_redcap_context: dict[str, Any]
    ) -> None:
        """Empty items list raises because production code doesn't guard it."""
        with pytest.raises(
            (pl.exceptions.ColumnNotFoundError, pl.exceptions.SchemaError)
        ):
            format_for_redcap(make_ml_data(), sample_redcap_context)

    def test_handles_text_response_type(
        self, sample_redcap_context: dict[str, Any]
    ) -> None:
        """Text items with a placeholder option are processed successfully."""
        ml_data = make_ml_data(
            items=[
                {
                    "id": "item1234-ab12-cd34-ef56-abcdef123456",
                    "name": "free_text_item",
                    "question": {"en": "Any comments?"},
                    "responseType": "text",
                    "responseValues": {
                        "options": [{"text": "", "value": 0, "score": 0}]
                    },
                }
            ],
            itemIds=["item1234-ab12-cd34-ef56-abcdef123456"],
            answer=[{"value": "This is a comment"}],
        )
        assert len(format_for_redcap(ml_data, sample_redcap_context)) > 0

    def test_handles_null_answer_value_raises(
        self, sample_redcap_context: dict[str, Any]
    ) -> None:
        """Null answer on singleSelect raises due to schema mismatch."""
        ml_data = make_ml_data(
            items=[
                {
                    "id": "item1234-ab12-cd34-ef56-abcdef123456",
                    "name": "skipped_item",
                    "question": {"en": "Skipped question?"},
                    "responseType": "singleSelect",
                    "responseValues": {
                        "options": [{"text": "Yes", "value": 0, "score": 1}]
                    },
                }
            ],
            itemIds=["item1234-ab12-cd34-ef56-abcdef123456"],
            answer=[{"value": None}],
        )
        with pytest.raises(
            (pl.exceptions.SchemaError, pl.exceptions.InvalidOperationError)
        ):
            format_for_redcap(ml_data, sample_redcap_context)

    def test_complete_derived_from_response_column(
        self, sample_decrypted_answer: CuriousDecryptedAnswer
    ) -> None:
        """Completion status is derived from the response column in the output."""
        context: dict[str, Any] = {
            "record_id": "00001",
            "source_secret_id": "00001_P",
            "invite_status": 3,
            "redcap_event_name": "curious_parent_arm_1",
            "respondent_id": SAMPLE_SUBJECT_ID,
        }
        for result in format_for_redcap(sample_decrypted_answer, context):
            assert str(result.output["curious_account_created_complete"][0]) in (
                "0",
                "2",
            )

    def test_complete_falls_back_to_context(
        self,
        sample_decrypted_answer: CuriousDecryptedAnswer,
        sample_redcap_context: dict[str, Any],
    ) -> None:
        """When context has 'complete' key, it's used for completion status."""
        for result in format_for_redcap(sample_decrypted_answer, sample_redcap_context):
            assert result.output["curious_account_created_complete"][0] is not None


# ============================================================================
# Tests: pull_data_from_curious
# ============================================================================


class TestPullDataFromCurious:
    """Tests for pull_data_from_curious."""

    @staticmethod
    def _call_with_respondents(
        respondents: list[dict[str, Any]],
        already_completed: list[str] | None = None,
        extra_patches: dict[str, Any] | None = None,
    ) -> pl.DataFrame:
        """
        Call pull_data_from_curious with mocked API response.

        Parameters
        ----------
        respondents
            List of respondent dicts for the API result.
        already_completed
            Record IDs to return from fetch_api_data (for update_already_completed).
        extra_patches
            Additional patches to apply (target → return_value or side_effect).

        """
        with patch_invitations_module() as mocks:
            mocks["requests_get"].return_value = _mock_http_response(
                json_data={"result": respondents}
            )
            mocks["fetch_api_data"].return_value = already_completed or []
            extra_ctx = extra_patches or {}
            patches = []
            for target, value in extra_ctx.items():
                p = (
                    patch(target, **value)
                    if isinstance(value, dict)
                    else patch(target, return_value=value)
                )
                patches.append(p)
                p.start()
            try:
                return pull_data_from_curious("token")
            finally:
                for p in patches:
                    p.stop()

    def test_returns_empty_df_when_no_results(self) -> None:
        """Empty API result produces empty DataFrame."""
        assert self._call_with_respondents([]).is_empty()

    def test_raises_on_http_error(self) -> None:
        """HTTP errors are propagated."""
        with patch_invitations_module() as mocks:
            mocks["requests_get"].return_value = _mock_http_response(
                500, raise_for_status=requests.HTTPError("500")
            )
            with pytest.raises(requests.HTTPError):
                pull_data_from_curious("token")

    def test_includes_all_respondents_with_recent_or_null_last_seen(self) -> None:
        """All respondents with None or recent lastSeen are included."""
        result = self._call_with_respondents(
            [
                make_api_respondent("00001_P", SAMPLE_SUBJECT_ID),
                make_api_respondent(
                    "00002_P",
                    "subj5678-ab12-cd34-ef56-abcdef123456",
                    last_seen="2020-01-01T00:00:00.000Z",
                ),
            ],
            extra_patches={
                f"{INVITATIONS_MOD}.update_already_completed": {
                    "side_effect": lambda df: df
                },
                f"{INVITATIONS_MOD}.yesterday_or_more_recent": {"return_value": True},
            },
        )
        assert result.shape[0] == 2

    def test_calls_update_already_completed(self) -> None:
        """update_already_completed is called when records exist."""
        with patch_invitations_module() as mocks:
            mocks["requests_get"].return_value = _mock_http_response(
                json_data={
                    "result": [make_api_respondent("00001_P", SAMPLE_SUBJECT_ID)]
                }
            )
            mocks["fetch_api_data"].return_value = []
            with patch(f"{INVITATIONS_MOD}.update_already_completed") as mock_update:
                mock_update.side_effect = lambda df: df
                pull_data_from_curious("token")
                mock_update.assert_called_once()

    def test_skips_non_parent_records(self) -> None:
        """Records without ``_P`` suffix are excluded."""
        assert self._call_with_respondents(
            [make_api_respondent("00001", SAMPLE_SUBJECT_ID)]
        ).is_empty()


# ============================================================================
# Tests: push_to_redcap
# ============================================================================


class TestPushToRedcap:
    """Tests for push_to_redcap function."""

    def _call(self, csv_data: str, mock_resp) -> int:
        with patch_invitations_module() as mocks:
            mocks["requests_post"].return_value = mock_resp
            # Mock deduplicate_dataframe to pass through data unchanged
            with patch(
                "hbnmigration.from_curious.invitations_to_redcap.deduplicate_dataframe"
            ) as mock_dedupe:
                # Make it return the same data with 0 duplicates
                mock_dedupe.side_effect = lambda df, *args, **kwargs: (df, 0)
                return push_to_redcap(csv_data)

    def test_successful_push(self) -> None:
        """Successful POST returns the JSON count."""
        assert (
            self._call("record_id,value\n001,test", _mock_http_response(json_data=1))
            == 1
        )

    def test_raises_on_failure(self) -> None:
        """HTTP errors are propagated."""
        resp = _mock_http_response(
            400,
            reason="Bad Request",
            text="Error",
            raise_for_status=requests.HTTPError("400"),
        )
        with pytest.raises(requests.HTTPError):
            self._call("record_id,value\nbad,data", resp)

    def test_sends_csv_data(self) -> None:
        """CSV data and format are included in POST payload."""
        csv_data = "record_id,redcap_event_name,value\n001,arm_1,test"
        with patch_invitations_module() as mocks:
            mocks["requests_post"].return_value = _mock_http_response(json_data=1)
            with patch(
                "hbnmigration.from_curious.invitations_to_redcap.deduplicate_dataframe"
            ) as mock_dedupe:
                mock_dedupe.side_effect = lambda df, *args, **kwargs: (df, 0)
                push_to_redcap(csv_data)
                call_data = mocks["requests_post"].call_args[1]["data"]
                # The CSV content should be the same
                assert "001" in call_data["data"] or "1," in call_data["data"]
                assert "arm_1" in call_data["data"]
                assert "test" in call_data["data"]

    def test_returns_json_count(self) -> None:
        """Return value is the JSON response (record count)."""
        assert (
            self._call("record_id,value\ndata,test", _mock_http_response(json_data=5))
            == 5
        )


# ============================================================================
# Tests: fetch_api_data overloads
# ============================================================================

_FETCH_PATCH = "hbnmigration.utility_functions.custom._fetch_api_data"


class TestFetchApiDataOverloads:
    """Tests for the new fetch_api_data overloads."""

    def test_returns_dataframe_by_default(self) -> None:
        """Default return type is pandas DataFrame."""
        with patch(_FETCH_PATCH) as m:
            m.return_value = pd.DataFrame({"record": ["001", "002"]})
            assert isinstance(fetch_api_data("url", {}, {}), pd.DataFrame)

    def test_returns_list_when_requested(self) -> None:
        """``return_type=list`` extracts the named column as a list."""
        with patch(_FETCH_PATCH) as m:
            m.return_value = pd.DataFrame({"record": ["001", "002"]})
            assert fetch_api_data("url", {}, {}, return_type=list, column="record") == [
                "001",
                "002",
            ]

    def test_list_returns_empty_for_missing_column(self) -> None:
        """Missing column returns empty list."""
        with patch(_FETCH_PATCH) as m:
            m.return_value = pd.DataFrame({"other_col": ["001"]})
            assert (
                fetch_api_data("url", {}, {}, return_type=list, column="record") == []
            )

    def test_raises_for_unsupported_type(self) -> None:
        """Unsupported return_type raises NotImplementedError."""
        with patch(_FETCH_PATCH) as m:
            m.return_value = pd.DataFrame()
            with pytest.raises(NotImplementedError):
                fetch_api_data("url", {}, {}, return_type=dict)  # type: ignore[arg-type]


# ============================================================================
# Tests: tsx (TypeScript runner)
# ============================================================================

_TSX_PATCH = "hbnmigration.utility_functions.typescript.subprocess.run"


class TestTsx:
    """Tests for the updated tsx function."""

    @staticmethod
    def _mock_subprocess(
        stdout: str = "", stderr: str = "", *, raises: Exception | None = None
    ) -> Mock:
        """Create a mock subprocess result."""
        result = Mock()
        result.stdout = stdout
        result.stderr = stderr
        if raises:
            result.check_returncode.side_effect = raises
        else:
            result.check_returncode = Mock()
        return result

    def test_raises_on_empty_stdout(self) -> None:
        """Empty stdout raises ValueError."""
        from hbnmigration.utility_functions.typescript import tsx

        with patch(_TSX_PATCH) as mock_run:
            mock_run.return_value = self._mock_subprocess()
            with pytest.raises(ValueError, match="no output"):
                tsx("script.ts")

    def test_raises_on_invalid_json(self) -> None:
        """Non-JSON stdout raises an exception."""
        from hbnmigration.utility_functions.typescript import tsx

        with patch(_TSX_PATCH) as mock_run:
            mock_run.return_value = self._mock_subprocess(stdout="not json")
            with pytest.raises(Exception):
                tsx("script.ts")

    def test_returns_parsed_json(self) -> None:
        """Valid JSON stdout is parsed and returned."""
        from hbnmigration.utility_functions.typescript import tsx

        with patch(_TSX_PATCH) as mock_run:
            mock_run.return_value = self._mock_subprocess(stdout='{"key": "value"}')
            assert tsx("script.ts") == {"key": "value"}

    def test_raises_on_nonzero_exit(self) -> None:
        """Non-zero exit code raises CalledProcessError."""
        import subprocess as sp

        from hbnmigration.utility_functions.typescript import tsx

        with patch(_TSX_PATCH) as mock_run:
            mock_run.return_value = self._mock_subprocess(
                stderr="Error", raises=sp.CalledProcessError(1, "tsx")
            )
            with pytest.raises(sp.CalledProcessError):
                tsx("script.ts")

    def test_passes_input_to_subprocess(self) -> None:
        """The ``_input`` argument is forwarded to subprocess."""
        from hbnmigration.utility_functions.typescript import tsx

        with patch(_TSX_PATCH) as mock_run:
            mock_run.return_value = self._mock_subprocess(stdout="[]")
            tsx("script.ts", _input="test input")
            assert mock_run.call_args[1]["input"] == "test input"


# ============================================================================
# Tests: main flow integration
# ============================================================================


# test_invitations_curious_to_redcap.py - Fix the test


class TestMainFlow:
    """Integration tests for main() workflow."""

    def _run_main(self, invitation_df: pl.DataFrame):
        """Run main with mocked dependencies."""
        with patch_invitations_module() as mocks:
            # Configure return values
            mocks["pull_data_from_curious"].return_value = invitation_df
            mocks["check_activity_responses"].return_value = invitation_df
            mocks["requests_post"].return_value = _mock_http_response(json_data=1)

            # Mock deduplicate_dataframe
            with patch(f"{INVITATIONS_MOD}.deduplicate_dataframe") as mock_dedupe:
                mock_dedupe.side_effect = lambda df, *args, **kwargs: (df, 0)

                main()
                return (
                    mocks["pull_data_from_curious"],
                    mocks["check_activity_responses"],
                    mocks["requests_post"],
                )

    def test_main_deduplicates_by_record_id(self) -> None:
        """Duplicate record_ids are deduplicated before push."""
        dup_df = pl.DataFrame(
            {
                "record_id": ["00001", "00001"],
                "source_secret_id": ["00001_P", "00001_P"],
                "invite_status": [3, 2],
                "redcap_event_name": ["curious_parent_arm_1"] * 2,
                "complete": ["0", "0"],
                "respondent_id": [SAMPLE_SUBJECT_ID] * 2,
            }
        )
        *_, mock_push = self._run_main(dup_df)
        assert mock_push.called
        call_data = mock_push.call_args[1]["data"]
        assert "data" in call_data
        csv_lines = [ln for ln in call_data["data"].strip().split("\n") if ln]
        assert len(csv_lines) == 2  # header + 1 data row
