"""Tests for invitations_to_redcap module."""

from contextlib import contextmanager
from typing import Any, cast, Iterator
from unittest.mock import Mock, patch

import polars as pl
import pytest
import requests

from hbnmigration.from_curious.config import Fields as CuriousFields
from hbnmigration.from_curious.invitations_to_redcap import (
    _add_child_suffix,
    _field_suffix_for,
    _instrument_for,
    _prefixed_field,
    _process_accounts,
    _response_field_for,
    _status_field_for,
    _strip_instrument_infix,
    ACCOUNT_CONTEXTS,
    AccountContext,
    check_activity_response,
    check_activity_responses,
    create_invitation_record,
    format_for_redcap,
    pull_data_from_curious,
    push_to_redcap,
    update_already_completed,
)
from hbnmigration.utility_functions import CuriousDecryptedAnswer

# Constants for testing
INVITATIONS_MOD = "hbnmigration.from_curious.invitations_to_redcap"
SAMPLE_APPLET_ID = "abcd1234-ab12-cd34-ef56-abcdef123456"
SAMPLE_ACTIVITY_ID = "actv1234-ab12-cd34-ef56-abcdef123456"
SAMPLE_SUBJECT_ID = "subj1234-ab12-cd34-ef56-abcdef123456"
SAMPLE_TOKEN = "test_token_12345"


def _mock_http_response(
    status_code: int = 200,
    json_data: Any = None,
    text: str = "",
    reason: str = "OK",
    raise_for_status: Exception | None = None,
) -> Mock:
    """Create a mock HTTP response."""
    mock_resp = Mock()
    mock_resp.status_code = status_code
    mock_resp.json.return_value = json_data
    mock_resp.text = text
    mock_resp.reason = reason
    if raise_for_status:
        mock_resp.raise_for_status.side_effect = raise_for_status
    else:
        mock_resp.raise_for_status.return_value = None
    return mock_resp


def _empty_invitation_schema() -> pl.DataFrame:
    """Create empty DataFrame with invitation record schema."""
    return pl.DataFrame(
        {
            "record_id": [],
            "source_secret_id": [],
            "invite_status": [],
            "redcap_event_name": [],
            "complete": [],
            "respondent_id": [],
        }
    )


def make_api_respondent(
    secret_id: str,
    subject_id: str,
    status: str = "invited",
    last_seen: str | None = None,
) -> dict[str, Any]:
    """Create a respondent dict as returned by Curious API."""
    return {
        "status": status,
        "lastSeen": last_seen,
        "details": [
            {
                "appletId": SAMPLE_APPLET_ID,
                "respondentSecretId": secret_id,
                "subjectId": subject_id,
            }
        ],
    }


# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def sample_respondent() -> dict[str, Any]:
    """Sample respondent dict from Curious API."""
    return make_api_respondent("resp_123", SAMPLE_SUBJECT_ID)


@pytest.fixture
def sample_redcap_context() -> dict[str, Any]:
    """Sample REDCap context for formatting."""
    return {
        "record_id": "00001",
        "source_secret_id": "resp_123",
        "invite_status": 3,
        "redcap_event_name": "admin_arm_1",
        "complete": "0",
        "respondent_id": SAMPLE_SUBJECT_ID,
        "instrument": "curious_account_created_responder",
        "account_context": "responder",
    }


@pytest.fixture
def sample_invitation_df() -> pl.DataFrame:
    """Sample invitation DataFrame."""
    return pl.DataFrame(
        {
            "record_id": ["00001", "00002"],
            "source_secret_id": ["resp_123", "resp_456"],
            "invite_status": [3, 2],
            "redcap_event_name": ["admin_arm_1", "admin_arm_1"],
            "complete": ["0", "0"],
            "respondent_id": [SAMPLE_SUBJECT_ID, SAMPLE_SUBJECT_ID],
        }
    )


def make_ml_data(
    items: list[dict] | None = None,
    item_ids: list[str] | None = None,
    answer: list[dict] | None = None,
) -> CuriousDecryptedAnswer:
    """Create a minimal CuriousDecryptedAnswer for testing."""
    return cast(
        CuriousDecryptedAnswer,
        {
            "submitId": "submit1234-ab12-cd34-ef56-abcdef123456",
            "version": "1.0.0",
            "startDatetime": "2024-01-01T12:00:00.000",
            "endDatetime": "2024-01-01T12:05:00.000",
            "respondentSecretId": "resp_123",
            "sourceSecretId": "resp_123",
            "items": items or [],
            "itemIds": item_ids or [],
            "answer": answer or [],
        },
    )


# ============================================================================
# Tests - context helpers
# ============================================================================


class TestContextHelpers:
    """Tests for account-context helper functions."""

    @pytest.mark.parametrize(
        "ctx,expected",
        [
            ("responder", "curious_account_created_responder"),
            ("child", "curious_account_created_child"),
        ],
    )
    def test_instrument_for(self, ctx: AccountContext, expected: str) -> None:
        """Verify instrument name for each context."""
        assert _instrument_for(ctx) == expected

    @pytest.mark.parametrize(
        "ctx,expected",
        [("responder", ""), ("child", "_c")],
    )
    def test_field_suffix_for(self, ctx: AccountContext, expected: str) -> None:
        """Verify field suffix for each context."""
        assert _field_suffix_for(ctx) == expected

    @pytest.mark.parametrize(
        "base,ctx,expected",
        [
            (
                "source_secret_id",
                "responder",
                "curious_account_created_source_secret_id",
            ),
            ("source_secret_id", "child", "curious_account_created_source_secret_id_c"),
            ("invite_status", "responder", "curious_account_created_invite_status"),
            ("invite_status", "child", "curious_account_created_invite_status_c"),
        ],
    )
    def test_prefixed_field(
        self, base: str, ctx: AccountContext, expected: str
    ) -> None:
        """Verify prefixed field construction."""
        assert _prefixed_field(base, ctx) == expected

    @pytest.mark.parametrize(
        "ctx,expected",
        [
            ("responder", "curious_account_created_invite_status"),
            ("child", "curious_account_created_invite_status_c"),
        ],
    )
    def test_status_field_for(self, ctx: AccountContext, expected: str) -> None:
        """Verify status field name for each context."""
        assert _status_field_for(ctx) == expected

    @pytest.mark.parametrize(
        "ctx,expected",
        [
            (
                "responder",
                "curious_account_created_account_created_response",
            ),
            (
                "child",
                "curious_account_created_account_created_response_c",
            ),
        ],
    )
    def test_response_field_for(self, ctx: AccountContext, expected: str) -> None:
        """Verify response field name for each context."""
        assert _response_field_for(ctx) == expected


# ============================================================================
# Tests - CuriousFields config
# ============================================================================


class TestCuriousFieldsConfig:
    """Tests for the Fields config class."""

    def test_responder_fields_include_common(self) -> None:
        """Responder fields should include common fields."""
        fields = CuriousFields.for_context("responder")
        assert "record_id" in fields
        assert "redcap_event_name" in fields

    def test_child_fields_include_common(self) -> None:
        """Child fields should include common fields."""
        fields = CuriousFields.for_context("child")
        assert "record_id" in fields
        assert "redcap_event_name" in fields

    def test_responder_fields_have_no_suffix(self) -> None:
        """Responder-specific fields should not have _c suffix."""
        fields = CuriousFields.for_context("responder")
        responder_only = [f for f in fields if f not in CuriousFields.common]
        assert all(not f.endswith("_c") for f in responder_only)

    def test_child_data_fields_have_suffix(self) -> None:
        """Child data fields should have _c suffix (except _complete)."""
        fields = CuriousFields.for_context("child")
        child_only = [f for f in fields if f not in CuriousFields.common]
        data_fields = [f for f in child_only if not f.endswith("_complete")]
        assert all(f.endswith("_c") for f in data_fields)

    def test_responder_includes_response_field(self) -> None:
        """Responder should include the account_created_response field."""
        fields = CuriousFields.for_context("responder")
        assert "curious_account_created_account_created_response" in fields

    def test_child_excludes_response_field(self) -> None:
        """Child should not include a response field."""
        fields = CuriousFields.for_context("child")
        assert not any("response" in f for f in fields)

    def test_responder_and_child_have_different_fields(self) -> None:
        """Responder and child field lists should differ."""
        responder = CuriousFields.for_context("responder")
        child = CuriousFields.for_context("child")
        assert set(responder) != set(child)


# ============================================================================
# Tests - create_invitation_record
# ============================================================================


class TestCreateInvitationRecord:
    """Tests for create_invitation_record function."""

    @patch(f"{INVITATIONS_MOD}.lookup_mrn_from_r_id")
    def test_returns_dict_for_valid_responder(
        self, mock_lookup: Mock, sample_respondent: dict[str, Any]
    ) -> None:
        """Verify all expected keys are present for responder account."""
        mock_lookup.return_value = "1234567"
        result = create_invitation_record(
            sample_respondent, SAMPLE_APPLET_ID, "responder", SAMPLE_TOKEN
        )
        assert result is not None
        assert result["record_id"] == "1234567"
        assert "curious_account_created_source_secret_id" in result
        assert "curious_account_created_invite_status" in result
        assert result["curious_account_created_source_secret_id"] == "resp_123"
        assert result["instrument"] == "curious_account_created_responder"
        assert result["redcap_event_name"] == "admin_arm_1"
        assert "curious_account_created_responder_complete" in result
        assert "respondent_id" in result

    def test_returns_dict_for_valid_child(
        self, sample_respondent: dict[str, Any]
    ) -> None:
        """Verify all expected keys are present for child account."""
        sample_respondent["details"][0]["respondentSecretId"] = "1234567"
        result = create_invitation_record(
            sample_respondent, SAMPLE_APPLET_ID, "child", SAMPLE_TOKEN
        )
        assert result is not None
        assert result["record_id"] == "1234567"
        assert "curious_account_created_source_secret_id_c" in result
        assert "curious_account_created_invite_status_c" in result
        assert result["curious_account_created_source_secret_id_c"] == "1234567"
        assert result["instrument"] == "curious_account_created_child"
        assert result["redcap_event_name"] == "admin_arm_1"
        assert "curious_account_created_child_complete" in result

    def test_returns_none_when_no_matching_applet(
        self, sample_respondent: dict[str, Any]
    ) -> None:
        """Non-matching applet ID should yield None."""
        assert (
            create_invitation_record(
                sample_respondent,
                "wrong_id-ab12-cd34-ef56-abcdef123456",
                "responder",
                SAMPLE_TOKEN,
            )
            is None
        )

    @patch(f"{INVITATIONS_MOD}.lookup_mrn_from_r_id")
    def test_returns_none_when_mrn_not_found(
        self, mock_lookup: Mock, sample_respondent: dict[str, Any]
    ) -> None:
        """Responder with no MRN found should return None."""
        mock_lookup.return_value = None
        assert (
            create_invitation_record(
                sample_respondent, SAMPLE_APPLET_ID, "responder", SAMPLE_TOKEN
            )
            is None
        )

    @patch(f"{INVITATIONS_MOD}.lookup_mrn_from_r_id")
    def test_handles_numeric_secret_id(
        self, mock_lookup: Mock, sample_respondent: dict[str, Any]
    ) -> None:
        """Numeric secret IDs should be handled correctly."""
        mock_lookup.return_value = "1234567"
        sample_respondent["details"][0]["respondentSecretId"] = "12345"
        result = create_invitation_record(
            sample_respondent, SAMPLE_APPLET_ID, "responder", SAMPLE_TOKEN
        )
        assert result is not None
        assert result["curious_account_created_source_secret_id"] == "12345"

    @patch(f"{INVITATIONS_MOD}.lookup_mrn_from_r_id")
    def test_uses_last_detail_when_multiple(
        self, mock_lookup: Mock, sample_respondent: dict[str, Any]
    ) -> None:
        """When multiple details match, use the last one."""
        mock_lookup.return_value = "9999999"
        sample_respondent["details"].append(
            {
                "appletId": SAMPLE_APPLET_ID,
                "respondentSecretId": "resp_999",
                "subjectId": "subj9999-ab12-cd34-ef56-abcdef123456",
            }
        )
        result = create_invitation_record(
            sample_respondent, SAMPLE_APPLET_ID, "responder", SAMPLE_TOKEN
        )
        assert result is not None
        assert result["curious_account_created_source_secret_id"] == "resp_999"
        assert result["respondent_id"] == "subj9999-ab12-cd34-ef56-abcdef123456"

    @patch(f"{INVITATIONS_MOD}.lookup_mrn_from_r_id")
    @pytest.mark.parametrize(
        "status,expected",
        [("not_invited", 1), ("pending", 2), ("invited", 3)],
    )
    def test_maps_invitation_statuses(
        self,
        mock_lookup: Mock,
        sample_respondent: dict[str, Any],
        status: str,
        expected: int,
    ) -> None:
        """Each Curious status maps to the correct numeric value."""
        mock_lookup.return_value = "1234567"
        sample_respondent["status"] = status
        result = create_invitation_record(
            sample_respondent, SAMPLE_APPLET_ID, "responder", SAMPLE_TOKEN
        )
        assert result is not None
        assert result["curious_account_created_invite_status"] == expected

    def test_empty_details_returns_none(
        self, sample_respondent: dict[str, Any]
    ) -> None:
        """Respondent with no details should return None."""
        sample_respondent["details"] = []
        assert (
            create_invitation_record(
                sample_respondent, SAMPLE_APPLET_ID, "responder", SAMPLE_TOKEN
            )
            is None
        )

    @patch(f"{INVITATIONS_MOD}.lookup_mrn_from_r_id")
    @pytest.mark.parametrize("ctx", ACCOUNT_CONTEXTS)
    def test_record_keys_match_fields_config(
        self, mock_lookup: Mock, sample_respondent: dict[str, Any], ctx: AccountContext
    ) -> None:
        """Record keys for REDCap fields should be a subset of Fields.for_context."""
        mock_lookup.return_value = "1234567"
        if ctx == "child":
            sample_respondent["details"][0]["respondentSecretId"] = "1234567"
        result = create_invitation_record(
            sample_respondent, SAMPLE_APPLET_ID, ctx, SAMPLE_TOKEN
        )
        assert result is not None
        valid_fields = set(CuriousFields.for_context(ctx))
        # internal-only keys that never go to REDCap
        internal = {"respondent_id", "instrument", "account_context"}
        redcap_keys = set(result.keys()) - internal
        assert redcap_keys.issubset(valid_fields), (
            f"Unexpected keys for {ctx}: {redcap_keys - valid_fields}"
        )


# ============================================================================
# Tests - update_already_completed
# ============================================================================


@contextmanager
def patch_invitations_module() -> Iterator[dict[str, Mock]]:
    """Context manager providing commonly-mocked targets."""
    targets = {
        "requests_get": f"{INVITATIONS_MOD}.requests.get",
        "requests_post": f"{INVITATIONS_MOD}.requests.post",
        "fetch_api_data": f"{INVITATIONS_MOD}.fetch_api_data",
        "get_applet_encryption": f"{INVITATIONS_MOD}.get_applet_encryption",
        "decrypt_single": f"{INVITATIONS_MOD}.decrypt_single",
        "curious_authenticate": f"{INVITATIONS_MOD}.curious_authenticate",
        "lookup_mrn_from_r_id": f"{INVITATIONS_MOD}.lookup_mrn_from_r_id",
    }
    patches = {name: patch(target) for name, target in targets.items()}
    mocks = {}
    for name, p in patches.items():
        mocks[name] = p.start()
    try:
        yield mocks
    finally:
        for p in patches.values():
            p.stop()


class TestUpdateAlreadyCompleted:
    """Tests for update_already_completed function."""

    @staticmethod
    def _run(df: pl.DataFrame, already_completed: list[str]) -> pl.DataFrame:
        """Run update_already_completed with mocked fetch_api_data."""
        with patch_invitations_module() as mocks:
            mocks["fetch_api_data"].return_value = already_completed
            return update_already_completed(df, "responder", SAMPLE_TOKEN)

    def test_filters_out_completed_records(
        self, sample_invitation_df: pl.DataFrame
    ) -> None:
        """Records already complete in REDCap should be filtered out."""
        result = self._run(sample_invitation_df, ["00001"])
        assert result.shape[0] == 1
        assert result["record_id"].to_list() == ["00002"]

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
                "source_secret_id": ["resp_123", None, "resp_789"],
                "invite_status": [3, None, 2],
                "redcap_event_name": ["admin_arm_1", None, "admin_arm_1"],
                "complete": ["0", None, "0"],
                "respondent_id": [SAMPLE_SUBJECT_ID, None, SAMPLE_SUBJECT_ID],
            }
        )
        assert self._run(df, []).shape[0] == 2


# ============================================================================
# Tests - check_activity_response
# ============================================================================


class TestCheckActivityResponse:
    """Tests for check_activity_response function."""

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
                "token", context, SAMPLE_APPLET_ID, SAMPLE_ACTIVITY_ID, "responder"
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
                "token",
                sample_redcap_context,
                SAMPLE_APPLET_ID,
                SAMPLE_ACTIVITY_ID,
                "responder",
            )
            call_args = mocks["requests_get"].call_args
            assert "targetSubjectId" in call_args[0][0]


# ============================================================================
# Tests - check_activity_responses
# ============================================================================


class TestCheckActivityResponses:
    """Tests for check_activity_responses function."""

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
                "token", df, SAMPLE_APPLET_ID, SAMPLE_ACTIVITY_ID, "responder"
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
                "token",
                sample_invitation_df,
                SAMPLE_APPLET_ID,
                SAMPLE_ACTIVITY_ID,
                "responder",
            )
            assert mock_check.call_count == len(sample_invitation_df)

    def test_concatenates_responses(self, sample_invitation_df: pl.DataFrame) -> None:
        """Output DataFrames from responses are concatenated."""
        named_output = Mock()
        named_output.output = pl.DataFrame({"record_id": ["00001"], "value": ["test"]})
        with patch(
            f"{INVITATIONS_MOD}.check_activity_response", return_value=[named_output]
        ):
            result = check_activity_responses(
                "token",
                sample_invitation_df,
                SAMPLE_APPLET_ID,
                SAMPLE_ACTIVITY_ID,
                "responder",
            )
            assert "value" in result.columns

    def test_handles_empty_df(self) -> None:
        """Empty input DataFrame produces empty output."""
        assert self._call(_empty_invitation_schema()).is_empty()


# ============================================================================
# Tests - format_for_redcap
# ============================================================================


class TestFormatForRedcap:
    """Tests for format_for_redcap function."""

    def test_returns_empty_list_for_empty_data(
        self, sample_redcap_context: dict[str, Any]
    ) -> None:
        """Empty dict input returns empty list."""
        assert (
            format_for_redcap(
                cast(CuriousDecryptedAnswer, {}), sample_redcap_context, "responder"
            )
            == []
        )

    def test_returns_empty_list_for_none_data(
        self, sample_redcap_context: dict[str, Any]
    ) -> None:
        """None input returns empty list."""
        assert (
            format_for_redcap(
                cast(CuriousDecryptedAnswer, None), sample_redcap_context, "responder"
            )
            == []
        )

    def test_produces_named_outputs(
        self,
        sample_decrypted_answer: CuriousDecryptedAnswer,
        sample_redcap_context: dict[str, Any],
    ) -> None:
        """Valid input produces NamedOutput objects with Polars DataFrames."""
        results = format_for_redcap(
            sample_decrypted_answer, sample_redcap_context, "responder"
        )
        assert len(results) > 0
        assert all(hasattr(r, "output") for r in results)
        assert all(isinstance(r.output, pl.DataFrame) for r in results)

    def test_output_contains_record_id(
        self,
        sample_decrypted_answer: CuriousDecryptedAnswer,
        sample_redcap_context: dict[str, Any],
    ) -> None:
        """Output DataFrame includes record_id from context."""
        for result in format_for_redcap(
            sample_decrypted_answer, sample_redcap_context, "responder"
        ):
            assert "record_id" in result.output.columns
            assert result.output["record_id"][0] == sample_redcap_context["record_id"]

    def test_output_contains_redcap_event_name(
        self,
        sample_decrypted_answer: CuriousDecryptedAnswer,
        sample_redcap_context: dict[str, Any],
    ) -> None:
        """Output DataFrame includes redcap_event_name from context."""
        for result in format_for_redcap(
            sample_decrypted_answer, sample_redcap_context, "responder"
        ):
            assert "redcap_event_name" in result.output.columns

    def test_child_adds_suffix_to_data_fields(
        self,
        sample_decrypted_answer: CuriousDecryptedAnswer,
        sample_redcap_context: dict[str, Any],
    ) -> None:
        """Child account context adds _c suffix to data fields (not complete)."""
        sample_redcap_context["instrument"] = "curious_account_created_child"
        for result in format_for_redcap(
            sample_decrypted_answer, sample_redcap_context, "child"
        ):
            data_fields = [
                col
                for col in result.output.columns
                if col.startswith("curious_account_created_")
                and not col.endswith("_complete")
            ]
            assert all(col.endswith("_c") for col in data_fields)

    @pytest.mark.parametrize("ctx", ACCOUNT_CONTEXTS)
    def test_output_only_contains_valid_fields(
        self,
        sample_decrypted_answer: CuriousDecryptedAnswer,
        sample_redcap_context: dict[str, Any],
        ctx: AccountContext,
    ) -> None:
        """Output columns should be a subset of Fields.for_context."""
        if ctx == "child":
            sample_redcap_context["instrument"] = "curious_account_created_child"
        valid = set(CuriousFields.for_context(ctx))
        for result in format_for_redcap(
            sample_decrypted_answer, sample_redcap_context, ctx
        ):
            assert set(result.output.columns).issubset(valid), (
                f"Unexpected columns for {ctx}: {set(result.output.columns) - valid}"
            )

    @pytest.mark.parametrize(
        "dt_str",
        [
            "2024-01-01T12:00:00.000Z",
            "2024-01-01T12:00:00.000",
            "2026-04-24T16:36:42.042000",
        ],
        ids=["3_frac_with_Z", "3_frac_no_Z", "6_frac_no_Z"],
    )
    def test_handles_various_datetime_formats(
        self,
        sample_redcap_context: dict[str, Any],
        dt_str: str,
    ) -> None:
        """format_for_redcap should not crash on various datetime formats."""
        ml_data = make_ml_data(
            items=[
                {
                    "id": "item1",
                    "name": "test_item",
                    "question": {"en": "Test?"},
                    "responseType": "singleSelect",
                    "responseValues": {
                        "options": [{"text": "Yes", "value": 0, "score": 1}]
                    },
                }
            ],
            answer=[{"value": 0}],
        )
        # Override datetimes with the parameterised format
        ml_data["startDatetime"] = dt_str
        ml_data["endDatetime"] = dt_str
        results = format_for_redcap(ml_data, sample_redcap_context, "responder")
        assert len(results) > 0


# ============================================================================
# Tests - pull_data_from_curious
# ============================================================================


class TestPullDataFromCurious:
    """Tests for pull_data_from_curious function."""

    @staticmethod
    def _call_with_respondents(
        respondents: list[dict[str, Any]],
        already_completed: list[str] | None = None,
        extra_patches: dict[str, Any] | None = None,
        account_context: AccountContext = "responder",
    ) -> pl.DataFrame:
        """Call pull_data_from_curious with mocked API response."""
        with patch(f"{INVITATIONS_MOD}.curious_variables") as mock_curious_vars:
            mock_curious_vars.applet_ids = {
                "Healthy Brain Network Questionnaires": SAMPLE_APPLET_ID
            }
            mock_curious_vars.owner_ids = {
                "Healthy Brain Network (HBN)": "owner_id_123"
            }
            with patch(
                f"{INVITATIONS_MOD}.yesterday_or_more_recent", return_value=True
            ):
                with patch_invitations_module() as mocks:
                    mocks["requests_get"].return_value = _mock_http_response(
                        json_data={"result": respondents}
                    )
                    mocks["fetch_api_data"].return_value = already_completed or []
                    mocks["lookup_mrn_from_r_id"].return_value = "1234567"
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
                        return pull_data_from_curious(
                            "token",
                            "Healthy Brain Network Questionnaires",
                            account_context,
                            SAMPLE_TOKEN,
                        )
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
                pull_data_from_curious(
                    "token",
                    "Healthy Brain Network Questionnaires",
                    "responder",
                    SAMPLE_TOKEN,
                )

    def test_includes_all_valid_respondents(self) -> None:
        """All valid respondents are included."""
        with patch(f"{INVITATIONS_MOD}.create_invitation_record") as mock_create:

            def create_record(respondent, applet_id, account_context, token):
                field_suffix = _field_suffix_for(account_context)
                instrument = _instrument_for(account_context)
                return {
                    "record_id": "1234567",
                    f"curious_account_created_source_secret_"
                    f"id{field_suffix}": respondent["details"][0]["respondentSecretId"],
                    f"curious_account_created_invite_status{field_suffix}": 3,
                    "redcap_event_name": "admin_arm_1",
                    f"{instrument}_complete": "0",
                    "respondent_id": respondent["details"][0]["subjectId"],
                    "instrument": instrument,
                    "account_context": account_context,
                }

            mock_create.side_effect = create_record
            with patch(f"{INVITATIONS_MOD}.update_already_completed") as mock_update:
                mock_update.side_effect = lambda df, *args: df
                result = self._call_with_respondents(
                    [
                        make_api_respondent("resp_123", SAMPLE_SUBJECT_ID),
                        make_api_respondent(
                            "resp_456", "subj5678-ab12-cd34-ef56-abcdef123456"
                        ),
                    ],
                    already_completed=[],
                )
                assert len(result) == 2

    def test_calls_update_already_completed(self) -> None:
        """update_already_completed is called when records exist."""
        with patch(f"{INVITATIONS_MOD}.curious_variables") as mock_curious_vars:
            mock_curious_vars.applet_ids = {
                "Healthy Brain Network Questionnaires": SAMPLE_APPLET_ID
            }
            mock_curious_vars.owner_ids = {
                "Healthy Brain Network (HBN)": "owner_id_123"
            }
            with patch_invitations_module() as mocks:
                mocks["requests_get"].return_value = _mock_http_response(
                    json_data={
                        "result": [make_api_respondent("resp_123", SAMPLE_SUBJECT_ID)]
                    }
                )
                mocks["fetch_api_data"].return_value = []
                mocks["lookup_mrn_from_r_id"].return_value = "1234567"
                with patch(
                    f"{INVITATIONS_MOD}.update_already_completed"
                ) as mock_update:
                    mock_update.side_effect = lambda df, *args: df
                    result = pull_data_from_curious(
                        "token",
                        "Healthy Brain Network Questionnaires",
                        "responder",
                        SAMPLE_TOKEN,
                    )
                    if not result.is_empty():
                        assert mock_update.called


# ============================================================================
# Tests - push_to_redcap
# ============================================================================


class TestPushToRedcap:
    """Tests for push_to_redcap function."""

    def _call(self, csv_data: str, mock_resp: Mock) -> int:
        """Call push_to_redcap with mocked HTTP POST."""
        with patch_invitations_module() as mocks:
            mocks["requests_post"].return_value = mock_resp
            with patch(f"{INVITATIONS_MOD}.deduplicate_dataframe") as mock_dedupe:
                mock_dedupe.side_effect = lambda df, *args, **kwargs: (df, 0)
                return push_to_redcap(csv_data, SAMPLE_TOKEN)

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
        csv_data = "record_id,redcap_event_name,value\n001,admin_arm_1,test"
        with patch_invitations_module() as mocks:
            mocks["requests_post"].return_value = _mock_http_response(json_data=1)
            with patch(f"{INVITATIONS_MOD}.deduplicate_dataframe") as mock_dedupe:
                mock_dedupe.side_effect = lambda df, *args, **kwargs: (df, 0)
                push_to_redcap(csv_data, SAMPLE_TOKEN)
                call_kwargs = mocks["requests_post"].call_args[1]
                assert "data" in call_kwargs
                assert call_kwargs["data"]["format"] == "csv"

    def test_returns_json_count(self) -> None:
        """Return value is the JSON response (record count)."""
        assert (
            self._call("record_id,value\ndata,test", _mock_http_response(json_data=5))
            == 5
        )

    def test_field_names_match_redcap_schema(self) -> None:
        """Verify field names match REDCap data dictionary."""
        with patch(f"{INVITATIONS_MOD}.lookup_mrn_from_r_id") as mock_lookup:
            mock_lookup.return_value = "1234567"
            # Test responder fields
            respondent = make_api_respondent("resp_123", SAMPLE_SUBJECT_ID)
            result_responder = create_invitation_record(
                respondent, SAMPLE_APPLET_ID, "responder", SAMPLE_TOKEN
            )
            assert result_responder is not None
            assert "curious_account_created_source_secret_id" in result_responder
            assert "curious_account_created_invite_status" in result_responder
            assert "curious_account_created_responder_complete" in result_responder
            assert "curious_account_created_source_secret_id_c" not in result_responder

            # Test child fields
            child_respondent = make_api_respondent("1234567", SAMPLE_SUBJECT_ID)
            result_child = create_invitation_record(
                child_respondent, SAMPLE_APPLET_ID, "child", SAMPLE_TOKEN
            )
            assert result_child is not None
            assert "curious_account_created_source_secret_id_c" in result_child
            assert "curious_account_created_invite_status_c" in result_child
            assert "curious_account_created_child_complete" in result_child
            assert "curious_account_created_source_secret_id" not in result_child

    def test_metadata_fields_removed_before_redcap_push(self) -> None:
        """Verify metadata fields are removed before pushing to REDCap."""
        test_df = pl.DataFrame(
            {
                "record_id": ["001"],
                "curious_account_created_source_secret_id": ["resp_123"],
                "curious_account_created_invite_status": [3],
                "redcap_event_name": ["admin_arm_1"],
                "curious_account_created_responder_complete": ["0"],
                "instrument": ["curious_account_created_responder"],
                "account_context": ["responder"],
                "respondent_id": ["subj_123"],
            }
        )
        with patch_invitations_module() as mocks:
            mocks["requests_post"].return_value = _mock_http_response(json_data=1)
            with patch(f"{INVITATIONS_MOD}.deduplicate_dataframe") as mock_dedupe:
                mock_dedupe.side_effect = lambda df, *args, **kwargs: (df, 0)
                push_to_redcap(test_df, SAMPLE_TOKEN)
                call_data = mocks["requests_post"].call_args[1]["data"]["data"]
                assert "instrument" not in call_data
                assert "account_context" not in call_data
                assert "respondent_id" not in call_data
                assert "record_id" in call_data
                assert "curious_account_created_source_secret_id" in call_data

    def test_metadata_fields_removed_before_deduplication(self) -> None:
        """Verify metadata fields are removed before calling deduplicate_dataframe."""
        test_df = pl.DataFrame(
            {
                "record_id": ["001"],
                "curious_account_created_source_secret_id": ["resp_123"],
                "curious_account_created_invite_status": [3],
                "redcap_event_name": ["admin_arm_1"],
                "curious_account_created_responder_complete": ["0"],
                "instrument": ["curious_account_created_responder"],
                "account_context": ["responder"],
                "respondent_id": ["subj_123"],
            }
        )
        with patch_invitations_module() as mocks:
            mocks["requests_post"].return_value = _mock_http_response(json_data=1)
            with patch(f"{INVITATIONS_MOD}.deduplicate_dataframe") as mock_dedupe:
                mock_dedupe.side_effect = lambda df, *args, **kwargs: (df, 0)
                push_to_redcap(test_df, SAMPLE_TOKEN)
                assert mock_dedupe.called
                called_df = mock_dedupe.call_args[0][0]
                assert "instrument" not in called_df.columns
                assert "account_context" not in called_df.columns
                assert "respondent_id" not in called_df.columns
                assert "record_id" in called_df.columns
                assert "curious_account_created_source_secret_id" in called_df.columns


# ============================================================================
# Tests - cache keys
# ============================================================================


class TestInvitationCacheKeys:
    """Test invitation-specific cache key creation."""

    def test_create_invitation_cache_key(self) -> None:
        """Test creating cache key for invitation."""
        from hbnmigration.from_curious.invitations_to_redcap import (
            create_invitation_cache_key,
        )

        assert create_invitation_cache_key("12345", "3", True) == "12345:3:1"

    def test_create_invitation_cache_key_no_response(self) -> None:
        """Test cache key with no response."""
        from hbnmigration.from_curious.invitations_to_redcap import (
            create_invitation_cache_key,
        )

        assert create_invitation_cache_key("12345", "2", False) == "12345:2:0"

    def test_process_accounts_uses_cache_keys(self, tmp_path: Any) -> None:
        """Test account processing uses composite cache keys."""
        from hbnmigration.utility_functions import DataCache

        cache = DataCache("test", ttl_minutes=5, cache_dir=str(tmp_path))
        with (
            patch(f"{INVITATIONS_MOD}.curious_authenticate"),
            patch(f"{INVITATIONS_MOD}.pull_data_from_curious") as mock_pull,
        ):
            mock_pull.return_value = pl.DataFrame()
            _process_accounts(
                "Test Applet", "responder", "token", "lookup_token", cache, 625
            )
            assert cache.get_stats()["total_entries"] == 0


# ============================================================================
# Tests - _process_accounts (unified)
# ============================================================================


class TestProcessAccounts:
    """Tests for the unified _process_accounts function."""

    @pytest.mark.parametrize("ctx", ACCOUNT_CONTEXTS)
    def test_logs_and_returns_on_auth_failure(
        self, tmp_path: Any, ctx: AccountContext, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Auth failure should log warning and return without error."""
        from hbnmigration.utility_functions import DataCache

        cache = DataCache("test", ttl_minutes=5, cache_dir=str(tmp_path))
        with patch(
            f"{INVITATIONS_MOD}.curious_authenticate",
            side_effect=KeyError("not configured"),
        ):
            _process_accounts("Fake Applet", ctx, "token", "lookup_token", cache, 625)
        assert "not configured" in caplog.text

    @pytest.mark.parametrize("ctx", ACCOUNT_CONTEXTS)
    def test_returns_on_empty_pull(
        self, tmp_path: Any, ctx: AccountContext, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Empty pull_data_from_curious should log and return."""
        from hbnmigration.utility_functions import DataCache

        cache = DataCache("test", ttl_minutes=5, cache_dir=str(tmp_path))
        with (
            patch(f"{INVITATIONS_MOD}.curious_authenticate"),
            patch(f"{INVITATIONS_MOD}.pull_data_from_curious") as mock_pull,
        ):
            mock_pull.return_value = pl.DataFrame()
            _process_accounts("Test Applet", ctx, "token", "lookup_token", cache, 625)
        assert f"No {ctx} invitations" in caplog.text


# ============================================================================
# Tests - _strip_instrument_infix
# ============================================================================


class TestStripInstrumentInfix:
    """Tests for _strip_instrument_infix."""

    @pytest.mark.parametrize(
        "col,ctx,expected",
        [
            (
                "curious_account_created_responder_account_created_response",
                "responder",
                "curious_account_created_account_created_response",
            ),
            (
                "curious_account_created_responder_source_secret_id",
                "responder",
                "curious_account_created_source_secret_id",
            ),
            (
                "curious_account_created_child_account_created_response",
                "child",
                "curious_account_created_account_created_response",
            ),
            (
                "curious_account_created_child_source_secret_id",
                "child",
                "curious_account_created_source_secret_id",
            ),
        ],
    )
    def test_strips_infix(self, col: str, ctx: AccountContext, expected: str) -> None:
        """Instrument infix should be removed from data fields."""
        assert _strip_instrument_infix(col, ctx) == expected

    @pytest.mark.parametrize("ctx", ACCOUNT_CONTEXTS)
    def test_preserves_complete_field(self, ctx: AccountContext) -> None:
        """_complete fields should be left untouched."""
        instrument = _instrument_for(ctx)
        complete = f"{instrument}_complete"
        assert _strip_instrument_infix(complete, ctx) == complete

    @pytest.mark.parametrize(
        "col",
        ["record_id", "redcap_event_name", "some_unrelated_field"],
    )
    def test_leaves_non_prefixed_columns_alone(self, col: str) -> None:
        """Columns without the curious prefix should pass through unchanged."""
        assert _strip_instrument_infix(col, "responder") == col

    def test_does_not_strip_wrong_context(self) -> None:
        """Responder infix should not be stripped when context is child."""
        col = "curious_account_created_responder_account_created_response"
        # child context looks for "child_" infix, not "responder_"
        assert _strip_instrument_infix(col, "child") == col


# ============================================================================
# Tests - _add_child_suffix
# ============================================================================


class TestAddChildSuffix:
    """Tests for _add_child_suffix."""

    def test_appends_suffix_to_data_field(self) -> None:
        """Data fields should get the suffix appended."""
        assert (
            _add_child_suffix("curious_account_created_source_secret_id", "_c")
            == "curious_account_created_source_secret_id_c"
        )

    def test_preserves_complete_field(self) -> None:
        """_complete fields should not get the suffix."""
        assert (
            _add_child_suffix("curious_account_created_child_complete", "_c")
            == "curious_account_created_child_complete"
        )

    @pytest.mark.parametrize(
        "col",
        ["record_id", "redcap_event_name"],
    )
    def test_preserves_structural_columns(self, col: str) -> None:
        """record_id and redcap_event_name should never get a suffix."""
        assert _add_child_suffix(col, "_c") == col

    def test_leaves_non_prefixed_columns_alone(self) -> None:
        """Columns without the curious prefix should pass through unchanged."""
        assert _add_child_suffix("some_other_field", "_c") == "some_other_field"
