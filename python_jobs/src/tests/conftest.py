"""Shared pytest configuration and fixtures."""

from collections.abc import Generator
from contextlib import contextmanager, ExitStack
import json
from pathlib import Path
import tempfile
from typing import Any, cast, TypedDict
from unittest.mock import AsyncMock, Mock, patch

import pandas as pd
import polars as pl
import pytest
import requests
from websockets.exceptions import InvalidStatus

from hbnmigration.from_redcap.config import Values
from hbnmigration.utility_functions.datatypes import (
    CuriousAlert,
    CuriousDecryptedAnswer,
    CuriousEncryption,
)

# ============================================================================
# Constants
# ============================================================================

DEFAULT_ENCRYPTION: CuriousEncryption = {
    "base": "base_value",
    "prime": "prime_value",
    "accountId": "account_001",
    "publicKey": "public_key_value",
}

DEFAULT_REDCAP_BASE_URL = "https://redcap.test/api/"

# Curious invitation test IDs
SAMPLE_APPLET_ID = "abcd1234-ab12-cd34-ef56-abcdef123456"
SAMPLE_ACTIVITY_ID = "actv1234-ab12-cd34-ef56-abcdef123456"
SAMPLE_RESPONDENT_ID = "resp1234-ab12-cd34-ef56-abcdef123456"
SAMPLE_SUBJECT_ID = "subj1234-ab12-cd34-ef56-abcdef123456"
SAMPLE_SUBMIT_ID = "smit1234-ab12-cd34-ef56-abcdef123456"

INVITATIONS_MOD = "hbnmigration.from_curious.invitations_to_redcap"

# ============================================================================
# File System Fixtures
# ============================================================================


@pytest.fixture
def temp_csv_file() -> Generator[Path, None, None]:
    """Create a temporary CSV file."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp:
        tmp_path = tmp.name
    yield Path(tmp_path)
    Path(tmp_path).unlink(missing_ok=True)


@pytest.fixture
def temp_excel_file() -> Generator[Path, None, None]:
    """Create a temporary Excel file."""
    with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
        tmp_path = tmp.name
    yield Path(tmp_path)
    Path(tmp_path).unlink(missing_ok=True)


@pytest.fixture
def temp_dir() -> Generator[Path, None, None]:
    """Create a temporary directory."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


# ============================================================================
# API Response Fixtures
# ============================================================================


def _create_mock_response(status_code: int, text: str) -> Mock:
    """Create mock API responses."""
    response = Mock()
    response.status_code = status_code
    response.text = text
    return response


@pytest.fixture
def mock_redcap_response() -> Mock:
    """Mock successful REDCap API response."""
    return _create_mock_response(requests.codes["okay"], "1")


@pytest.fixture
def mock_ripple_response() -> Mock:
    """Mock successful Ripple API response."""
    return _create_mock_response(requests.codes["okay"], "Success")


# ============================================================================
# Participant Data Fixtures - Base Factory
# ============================================================================


def create_participant_df(
    global_ids: list[str] | None = None,
    custom_ids: list[int] | None = None,
    first_names: list[str] | None = None,
    last_names: list[str] | None = None,
    consent_forms: list[str] | None = None,
    contact_types: list[str] | None = None,
    contact_info: list[str] | None = None,
    import_types: list[str] | None = None,
    **kwargs: Any,
) -> pd.DataFrame:
    """Create participant DataFrames with flexible defaults."""
    length = len(global_ids or custom_ids or first_names or [1])
    data: dict[str, Any] = {
        "globalId": global_ids or [f"CUSTOM{i:03d}" for i in range(1, length + 1)],
        "customId": custom_ids or list(range(99999, 99999 + length)),
        "firstName": first_names or ["Custom"] * length,
        "lastName": last_names or ["Participant"] * length,
        "cv.consent_form": consent_forms or ["Send to RedCap"] * length,
        "contact.1.infos.1.contactType": contact_types or ["email"] * length,
        "contact.1.infos.1.information": contact_info
        or [f"custom{i}@test.com" for i in range(1, length + 1)],
        "importType": import_types or ["HBN - Main"] * length,
    }
    data.update(kwargs)
    return pd.DataFrame(data)


def create_redcap_eav_df(
    records: list[str] | None = None,
    field_names: list[str] | None = None,
    values: list[str] | None = None,
    repeat_instruments: list[str] | None = None,
    repeat_instances: list[Any] | None = None,
    event_names: list[str] | None = None,
) -> pd.DataFrame:
    """Create REDCap EAV format DataFrames with flexible defaults."""
    if not any([records, field_names, values]):
        return pd.DataFrame(
            {
                "record": pd.Series([], dtype=str),
                "field_name": pd.Series([], dtype=str),
                "value": pd.Series([], dtype=str),
                "redcap_repeat_instrument": pd.Series([], dtype=str),
                "redcap_repeat_instance": pd.Series([], dtype=str),
            }
        )
    length = len(records or field_names or values or [0])
    data: dict[str, Any] = {
        "record": records or [""] * length,
        "field_name": field_names or [""] * length,
        "value": values or [""] * length,
        "redcap_repeat_instrument": repeat_instruments or [""] * length,
        "redcap_repeat_instance": repeat_instances or [""] * length,
    }
    if event_names is not None:
        data["redcap_event_name"] = event_names
    return pd.DataFrame(data)


def create_curious_participant_df(
    secret_user_ids: list[str] | None = None,
    tags: list[str] | None = None,
    first_names: list[str] | None = None,
    last_names: list[str] | None = None,
    **kwargs: Any,
) -> pd.DataFrame:
    """Create Curious participant DataFrames with flexible defaults."""
    length = len(secret_user_ids or tags or first_names or [1])
    default_tags = tags or ["child"] * length
    account_types_list = ["full" if t == "parent" else "limited" for t in default_tags]
    data: dict[str, Any] = {
        "secretUserId": secret_user_ids or [f"{i:05d}" for i in range(1, length + 1)],
        "tag": default_tags,
        "accountType": account_types_list,
        "firstName": first_names or ["Test"] * length,
        "lastName": last_names or ["User"] * length,
        "nickname": [None] * length,
        "role": ["respondent"] * length,
        "language": ["en"] * length,
    }
    data.update(kwargs)
    return pd.DataFrame(data)


# ============================================================================
# Alert Data Factories
# ============================================================================


def create_alert_df(
    records: list[str] | None = None,
    field_names: list[str] | None = None,
    values: list[str] | None = None,
    events: list[str] | None = None,
) -> pd.DataFrame:
    """
    Create alert DataFrames for testing.

    If no arguments provided, returns empty DataFrame with correct schema.
    Otherwise wraps create_redcap_eav_df with event_names mapping.
    """
    result = create_redcap_eav_df(
        records=records,
        field_names=field_names,
        values=values,
        event_names=events,
    )
    if "redcap_event_name" not in result.columns:
        result["redcap_event_name"] = pd.Series([], dtype=str) if result.empty else ""
    return result


def create_curious_alert(
    alert_id: str,
    secret_id: str,
    activity_item_id: str,
    message: str,
    respondent_id: str,
    subject_id: str,
    account_id: str = "account_001",
    **kwargs: Any,
) -> CuriousAlert:
    """Create CuriousAlert test data."""
    encryption = DEFAULT_ENCRYPTION.copy()
    encryption["accountId"] = account_id
    return {
        "id": alert_id,
        "isWatched": kwargs.get("isWatched", False),
        "appletId": kwargs.get("appletId", "hbn_applet_id"),
        "appletName": kwargs.get("appletName", "HBN Questionnaires"),
        "version": kwargs.get("version", "1.0.0"),
        "secretId": secret_id,
        "activityId": kwargs.get("activityId", "baseline_activity"),
        "activityItemId": activity_item_id,
        "message": message,
        "createdAt": kwargs.get("createdAt", "2024-01-01T00:00:00Z"),
        "answerId": kwargs.get("answerId", f"answer_{alert_id}"),
        "encryption": encryption,
        "workspace": kwargs.get("workspace", "workspace_1"),
        "respondentId": respondent_id,
        "subjectId": subject_id,
        "type": kwargs.get("type", "answer"),
    }


def create_alert_metadata(field_names: list[str], choices: list[str]) -> pd.DataFrame:
    """Create alert metadata."""
    field_types = ["text"] + ["radio"] * (len(field_names) - 1)
    return pd.DataFrame(
        {
            "field_name": field_names,
            "field_type": field_types,
            "select_choices_or_calculations": choices,
        }
    )


# ============================================================================
# Curious Invitation Factories
# ============================================================================


def make_api_respondent(
    secret_id: str,
    subject_id: str,
    status: str = "invited",
    last_seen: str | None = None,
    applet_id: str = SAMPLE_APPLET_ID,
) -> dict[str, Any]:
    """
    Build a respondent dict matching the Curious invitation API shape.

    Parameters
    ----------
    secret_id
        The ``respondentSecretId`` value.
    subject_id
        The ``subjectId`` value.
    status
        Invitation status string.
    last_seen
        ISO datetime or ``None``.
    applet_id
        Applet ID for the detail record.

    Returns
    -------
    dict[str, Any]

    """
    return {
        "status": status,
        "lastSeen": last_seen,
        "details": [
            {
                "appletId": applet_id,
                "respondentSecretId": secret_id,
                "subjectId": subject_id,
            }
        ],
    }


def make_ml_data(**overrides: Any) -> CuriousDecryptedAnswer:
    """
    Build a minimal CuriousDecryptedAnswer with overrides.

    Parameters
    ----------
    **overrides
        Keys to override in the base dict.

    Returns
    -------
    CuriousDecryptedAnswer

    """
    base: dict[str, Any] = {
        "activityId": SAMPLE_ACTIVITY_ID,
        "activityHistoryId": "hist1234-ab12-cd34-ef56-abcdef123456",
        "answerId": "answ1234-ab12-cd34-ef56-abcdef123456",
        "createdAt": "2024-06-01T12:00:00.000",
        "endDatetime": "2024-06-01T12:05:00.000",
        "flowHistoryId": None,
        "id": "id001234-ab12-cd34-ef56-abcdef123456",
        "identifier": None,
        "itemIds": [],
        "items": [],
        "migratedData": None,
        "reviewCount": {},
        "sourceSubject": {},
        "startDatetime": "2024-06-01T12:00:00.000",
        "submitId": SAMPLE_SUBMIT_ID,
        "subscaleSetting": None,
        "version": "1.0.0",
        "userPublicKey": "fake_public_key",
        "answer": [],
        "events": [],
        "respondentSecretId": "00001_P",
        "sourceSecretId": "00001_P",
    }
    base.update(overrides)
    return cast(CuriousDecryptedAnswer, base)


@contextmanager
def patch_invitations_module(
    **overrides: str,
) -> Generator[dict[str, Mock], None, None]:
    """
    Patch common dependencies in the invitations_to_redcap module.

    Parameters
    ----------
    **overrides
        Additional or replacement patch paths keyed by mock name.

    Yields
    ------
    dict[str, Mock]
        Dictionary of mock objects keyed by name.

    """
    default_patches = {
        "curious_variables": f"{INVITATIONS_MOD}.curious_variables",
        "redcap_variables": f"{INVITATIONS_MOD}.redcap_variables",
        "requests_get": f"{INVITATIONS_MOD}.requests.get",
        "requests_post": f"{INVITATIONS_MOD}.requests.post",
        "fetch_api_data": f"{INVITATIONS_MOD}.fetch_api_data",
        "get_applet_encryption": f"{INVITATIONS_MOD}.get_applet_encryption",
        "decrypt_single": f"{INVITATIONS_MOD}.decrypt_single",
        "endpoints": f"{INVITATIONS_MOD}.Endpoints",
    }
    default_patches.update(overrides)
    with ExitStack() as stack:
        mocks = {
            name: stack.enter_context(patch(path))
            for name, path in default_patches.items()
        }
        cv = mocks["curious_variables"]
        cv.headers.return_value = {"Content-Type": "application/json"}
        cv.applet_ids = {"Healthy Brain Network Questionnaires": SAMPLE_APPLET_ID}
        cv.activity_ids = {"Curious Account Created": SAMPLE_ACTIVITY_ID}
        cv.owner_ids = {
            "Healthy Brain Network (HBN)": "owner123-ab12-cd34-ef56-abcdef123456"
        }
        cv.Credentials.hbn_mindlogger = {"username": "u", "password": "p"}
        cv.AppletCredentials.hbn_mindlogger = {
            "Healthy Brain Network Questionnaires": {"applet_password": "secret"}
        }
        ep = mocks["endpoints"]
        ep.Curious.invitation_statuses.return_value = (
            "https://curious.test/api/invitations"
        )
        ep.Curious.applet.return_value = "https://curious.test/api/applet"
        ep.Curious.applet_activity_answers_list.return_value = (
            "https://curious.test/api/answers"
        )
        ep.Redcap.base_url = DEFAULT_REDCAP_BASE_URL
        rv = mocks["redcap_variables"]
        rv.headers = {"Content-Type": "application/x-www-form-urlencoded"}
        rv.Tokens.pid744 = "token_744"
        rv.Endpoints.return_value.base_url = DEFAULT_REDCAP_BASE_URL
        yield mocks


# ============================================================================
# Curious Invitation Fixtures
# ============================================================================


@pytest.fixture
def sample_respondent_detail() -> dict[str, str]:
    """Return a respondent detail dict as returned by the Curious API."""
    return {
        "appletId": SAMPLE_APPLET_ID,
        "respondentSecretId": "00001_P",
        "subjectId": SAMPLE_SUBJECT_ID,
    }


@pytest.fixture
def sample_respondent(
    sample_respondent_detail: dict[str, str],
) -> dict[str, Any]:
    """Return a respondent dict from the Curious invitation status API."""
    return {
        "status": "invited",
        "lastSeen": None,
        "details": [sample_respondent_detail],
    }


@pytest.fixture
def sample_redcap_context() -> dict[str, Any]:
    """Return a REDCap context dict for format_for_redcap."""
    return {
        "record_id": "00001",
        "source_secret_id": "00001_P",
        "invite_status": 3,
        "redcap_event_name": "curious_parent_arm_1",
        "complete": "0",
        "respondent_id": SAMPLE_SUBJECT_ID,
    }


@pytest.fixture
def sample_decrypted_answer() -> CuriousDecryptedAnswer:
    """
    Return a minimal CuriousDecryptedAnswer for testing.

    Note: datetimes must NOT have trailing 'Z' — the production code uses
    strptime with format ``%Y-%m-%dT%H:%M:%S%.f`` which does not parse 'Z'.
    """
    return make_ml_data(
        itemIds=["item1234-ab12-cd34-ef56-abcdef123456"],
        items=[
            {
                "id": "item1234-ab12-cd34-ef56-abcdef123456",
                "name": "account_created",
                "question": {
                    "en": (
                        "Please click below to confirm that you have "
                        "created a Curious account"
                    )
                },
                "responseType": "singleSelect",
                "responseValues": {
                    "options": [
                        {
                            "text": "I confirm that I have created a Curious account",
                            "value": 0,
                            "score": 1,
                        },
                        {"text": "No", "value": 1, "score": 0},
                    ]
                },
            }
        ],
        answer=[{"value": 0}],
    )


@pytest.fixture
def sample_invitation_df() -> pl.DataFrame:
    """Return a Polars DataFrame representing invitation records."""
    return pl.DataFrame(
        {
            "record_id": ["00001", "00002"],
            "source_secret_id": ["00001_P", "00002_P"],
            "invite_status": [3, 2],
            "redcap_event_name": [
                "curious_parent_arm_1",
                "curious_parent_arm_1",
            ],
            "complete": ["0", "0"],
            "respondent_id": [
                SAMPLE_SUBJECT_ID,
                "subj5678-ab12-cd34-ef56-abcdef123456",
            ],
        }
    )


# ============================================================================
# Participant Data Fixtures
# ============================================================================


@pytest.fixture
def participant_with_email() -> pd.DataFrame:
    """Return generic participant with email contact."""
    return create_participant_df(
        global_ids=["TEST001"],
        custom_ids=[12345],
        first_names=["Test"],
        contact_types=["email"],
        contact_info=["test@swamp.com"],
    )


@pytest.fixture
def participant_without_email() -> pd.DataFrame:
    """Return generic participant without email contact."""
    return create_participant_df(
        global_ids=["TEST002"],
        custom_ids=[67890],
        first_names=["NoEmail"],
        contact_types=["phone"],
        contact_info=["555-0123"],
    )


@pytest.fixture
def send_to_redcap_participant() -> pd.DataFrame:
    """Return participant with 'Send to RedCap' consent status."""
    return create_participant_df(
        global_ids=["TEST003"],
        custom_ids=[99999],
        first_names=["Ready"],
        consent_forms=["Send to RedCap"],
    )


@pytest.fixture
def swamp_thing_participant() -> pd.DataFrame:
    """Return Dr. Alec Holland's data."""
    return create_participant_df(
        global_ids=["ST001"],
        custom_ids=[12345],
        first_names=["Alec"],
        last_names=["Holland"],
        contact_info=["alec.holland@swampthing.com"],
        import_types=["HBN - Main"],
    )


@pytest.fixture
def parliament_of_trees_participants() -> pd.DataFrame:
    """Provide multiple Parliament of Trees members."""
    return create_participant_df(
        global_ids=["ST001", "AA001", "TE001"],
        custom_ids=[12345, 67890, 11111],
        first_names=["Alec", "Abby", "Tefé"],
        last_names=["Holland", "Arcane", "Holland"],
        contact_info=[
            "alec@swamp.com",
            "abby@parliament.org",
            "tefe@green.org",
        ],
        import_types=["HBN - Main", "HBN - Main", "HBN - Waitlist"],
    )


@pytest.fixture
def sample_ripple_data() -> pd.DataFrame:
    """Return Ripple data with multiple participants."""
    return create_participant_df(
        global_ids=["ST001", "AA001", "TE001", "WOO001"],
        custom_ids=[12345, 67890, 11111, 22222],
        first_names=["Alec", "Abby", "Tefé", "Woodrue"],
        last_names=["Holland", "Arcane", "Holland", "Jason"],
        contact_info=[
            "alec@swamp.com",
            "abby@parliament.org",
            "tefe@green.org",
            "woodrue@floronic.com",
        ],
        import_types=[
            "HBN - Main",
            "HBN - Waitlist",
            "HBN - Main",
            "HBN - Waitlist",
        ],
    )


@pytest.fixture
def anton_arcane_corrupted_data() -> pd.DataFrame:
    """Provide corrupted / rejected participant data."""
    return create_participant_df(
        global_ids=["ANT001"],
        custom_ids=[66666],
        first_names=["Anton"],
        last_names=["Arcane"],
        consent_forms=["Do Not Send"],
        contact_types=["phone"],
        contact_info=["666-666-6666"],
        import_types=["HBN - Rejected"],
    )


@pytest.fixture
def mock_redcap_existing_subjects() -> pd.DataFrame:
    """Mock existing REDCap subjects."""
    return pd.DataFrame({"mrn": [12345, 67890], "record_id": [1, 2]})


@pytest.fixture
def incoming_subjects_mixed() -> pd.DataFrame:
    """Return subjects with mix of new and existing."""
    return pd.DataFrame(
        {
            "record_id": [999, 998, 997],
            "mrn": [12345, 67890, 99001],
            "email_consent": [
                "alec@swamp.com",
                "abby@parliament.org",
                "bella@garden.green",
            ],
        }
    )


@pytest.fixture
def bella_garten_participant() -> pd.DataFrame:
    """Return data for the Gardener."""
    return create_participant_df(
        global_ids=["BG001"],
        custom_ids=[99001],
        first_names=["Bella"],
        last_names=["Garten"],
        contact_info=["bella@garden.green"],
        import_types=["HBN - Main"],
    )


# ============================================================================
# REDCap Data Fixtures (EAV Format)
# ============================================================================


@pytest.fixture
def sample_redcap_data() -> pd.DataFrame:
    """Sample REDCap data in EAV format from PID 247."""
    return create_redcap_eav_df(
        records=["001", "001", "001", "002", "002", "002"],
        field_names=[
            "intake_ready",
            "participant_name",
            "permission_collab",
        ]
        * 2,
        values=[
            Values.PID247.intake_ready["Ready to Send to Intake Redcap"],
            "Alec Holland",
            Values.PID247.permission_collab[
                "NO, you may not share my child's records."
            ],
            Values.PID247.intake_ready["Ready to Send to Intake Redcap"],
            "Abby Arcane",
            Values.PID247.permission_collab["YES, you may share my child's records."],
        ],
    )


@pytest.fixture
def empty_redcap_data() -> pd.DataFrame:
    """Empty DataFrame representing no data from REDCap."""
    return create_redcap_eav_df()


@pytest.fixture
def expected_transformed_data() -> pd.DataFrame:
    """Return expected data after transformation for PID 744."""
    return pd.DataFrame(
        {
            "record": ["001", "001", "002", "002"],
            "field_name": ["participant_full_name", "permission_collab"] * 2,
            "value": [
                "Alec Holland",
                Values.PID744.permission_collab["No"],
                "Abby Arcane",
                Values.PID744.permission_collab["Yes"],
            ],
        }
    )


# ============================================================================
# REDCap Data Fixtures - Curious Format
# ============================================================================


@pytest.fixture
def sample_redcap_curious_data() -> pd.DataFrame:
    """Sample REDCap data ready for Curious transfer."""
    return create_redcap_eav_df(
        records=["001"] * 5,
        field_names=[
            "mrn",
            "enrollment_complete",
            "consent_parent_first_name",
            "consent_child_first_name",
            "parent_involvement___1",
        ],
        values=["12345", "1", "Alec", "Tefé", "1"],
    )


@pytest.fixture
def parliament_curious_redcap_data() -> pd.DataFrame:
    """Parliament of Trees REDCap data for Curious transfer."""
    return create_redcap_eav_df(
        records=["ST001", "ST001", "AA001", "AA001"],
        field_names=["mrn", "parent_involvement___1"] * 2,
        values=["12345", "1", "67890", "1"],
    )


@pytest.fixture
def formatted_curious_data() -> pd.DataFrame:
    """Sample formatted data ready for Curious API."""
    return pd.DataFrame(
        {
            "secretUserId": ["00001", "00001_P"],
            "accountType": ["limited", "full"],
            "firstName": ["Tefé", "Alec"],
            "lastName": ["Holland", "Holland"],
            "nickname": [None, None],
            "role": ["respondent", "respondent"],
            "tag": ["child", "parent"],
            "language": ["en", "en"],
        }
    )


@pytest.fixture
def multi_record_curious_data() -> pd.DataFrame:
    """Multiple records formatted for Curious."""
    return pd.DataFrame(
        {
            "secretUserId": ["00001", "00001_P", "00002", "00002_P"],
            "accountType": ["limited", "full", "limited", "full"],
            "firstName": ["Tefé", "Alec", "Constantine", "Abby"],
            "lastName": ["Holland", "Holland", "Arcane", "Arcane"],
            "nickname": [None] * 4,
            "role": ["respondent"] * 4,
            "tag": ["child", "parent", "child", "parent"],
            "language": ["en"] * 4,
        }
    )


# ============================================================================
# Mock Configuration
# ============================================================================


def _create_mock_redcap_variables(
    pid247: str = "token_247",
    pid625: str = "token_625",
    pid744: str = "token_744",
    pid757: str = "token_757",
    base_url: str = DEFAULT_REDCAP_BASE_URL,
) -> Mock:
    """Create a standardized mock redcap_variables object."""
    mock_vars = Mock()
    mock_vars.Tokens.pid247, mock_vars.Tokens.pid625 = pid247, pid625
    mock_vars.Tokens.pid744, mock_vars.Tokens.pid757 = pid744, pid757
    mock_vars.headers = {"Content-Type": "application/x-www-form-urlencoded"}
    mock_vars.Endpoints = Mock()
    mock_vars.Endpoints.return_value.base_url = base_url
    return mock_vars


@pytest.fixture
def mock_curious_variables() -> Mock:
    """Mock curious_variables configuration."""
    mock_vars = Mock()
    mock_vars.headers.return_value = {"Content-Type": "application/json"}
    mock_vars.applet_ids = {"Healthy Brain Network Questionnaires": "test_applet_id"}
    mock_creds = Mock()
    mock_creds.hbn_mindlogger = Mock(username="test_user", password="test_pass")
    mock_vars.Credentials = mock_creds
    mock_tokens = Mock()
    mock_tokens.access = "test_access_token"
    mock_tokens.endpoints = Mock()
    mock_tokens.endpoints.base_url = "https://curious.test/api/"
    mock_vars.Tokens = Mock(return_value=mock_tokens)
    mock_endpoints = Mock()
    mock_endpoints.base_url = "https://curious.test/api/"
    mock_vars.Endpoints = Mock(return_value=mock_endpoints)
    return mock_vars


@pytest.fixture
def mock_redcap_variables_curious() -> Mock:
    """Mock redcap_variables for Curious transfer."""
    return _create_mock_redcap_variables()


@pytest.fixture
def mock_ripple_variables() -> Mock:
    """Mock ripple_variables configuration."""
    mock_vars = Mock()
    mock_vars.study_ids = {
        "HBN - Main": "main_study_id",
        "HBN - Waitlist": "waitlist_study_id",
    }
    mock_vars.column_dict.return_value = {}
    mock_vars.headers = {"import": {"Content-Type": "application/octet-stream"}}
    return mock_vars


@pytest.fixture
def mock_redcap_variables() -> Mock:
    """Mock redcap_variables configuration."""
    return _create_mock_redcap_variables()


@pytest.fixture
def setup_redcap_mocks(mock_redcap_variables: Mock, temp_csv_file: Path) -> Mock:
    """Set up common redcap variable mocks with temp file."""
    mock_redcap_variables.redcap_import_file = temp_csv_file
    return mock_redcap_variables


@pytest.fixture
def mock_endpoints() -> Mock:
    """Mock Endpoints configuration."""
    mock = Mock()
    mock.Ripple.import_data.return_value = "https://ripple.swamp.org/import"
    mock.Ripple.export_from_ripple.return_value = pd.DataFrame()
    mock.REDCap.base_url = "https://redcap.swamp.org/api/"
    return mock


@pytest.fixture
def mock_all_ripple_deps(
    mock_ripple_variables: Mock, mock_endpoints: Mock
) -> dict[str, Mock]:
    """Set up all common Ripple dependencies."""
    return {"endpoints": mock_endpoints, "variables": mock_ripple_variables}


@pytest.fixture
def mock_main_workflow_deps(
    mock_redcap_variables: Mock, temp_excel_file: Path
) -> dict[str, Any]:
    """Set up dependencies for main workflow tests."""
    return {"vars": mock_redcap_variables, "excel_file": temp_excel_file}


# ============================================================================
# Excel File Fixtures
# ============================================================================


@pytest.fixture
def excel_file_with_data(temp_excel_file: Path) -> Path:
    """Create Excel file with test data."""
    pd.DataFrame(
        {
            "globalId": ["ST001"],
            "cv.consent_form": ["consent_form_created_in_redcap"],
        }
    ).to_excel(temp_excel_file, index=False)
    return temp_excel_file


# ============================================================================
# Import Testing Fixtures
# ============================================================================


@pytest.fixture
def mock_importable_module() -> Mock:
    """Create a mock module with test attributes."""
    mock_mod = Mock()
    mock_mod.TestClass = Mock
    mock_mod.TestClass.__name__ = "TestClass"
    mock_mod.test_function = lambda x: x * 2
    mock_mod.TEST_CONSTANT = "test_value"
    return mock_mod


class FallbackDataDict(TypedDict):
    """Provide typing for fallback data dict."""

    parliament: dict[str, list[str]]
    avatars: list[str]
    members: list[str]


@pytest.fixture
def swamp_thing_fallback_data() -> FallbackDataDict:
    """Store complex fallback data structure for testing."""
    return {
        "parliament": {
            "trees": ["Yggdrasil", "Ghost Orchid"],
            "stones": ["Parliament of Stones"],
            "waves": ["Parliament of Waves"],
        },
        "avatars": ["Swamp Thing", "Black Orchid", "Poison Ivy"],
        "members": ["Alec Holland", "Abby Arcane", "Tefé Holland"],
    }


@pytest.fixture
def green_realm_config() -> dict[str, Any]:
    """Mock configuration data for API testing."""
    return {
        "api_key": "TEST_KEY",
        "endpoint": "https://green.realm/api",
        "timeout": 30,
        "retry_attempts": 3,
    }


@pytest.fixture
def mock_parliament_object() -> Mock:
    """Mock Parliament of Trees object."""
    mock_parliament = Mock()
    mock_parliament.members = ["Alec Holland", "Ghost Orchid", "Yggdrasil"]
    mock_parliament.collective_consciousness = True
    mock_parliament.green_connection = Mock()
    return mock_parliament


# ============================================================================
# Workflow Patching Fixtures
# ============================================================================


@pytest.fixture
def patched_main_workflow() -> Generator[dict[str, Mock], None, None]:
    """Provide context manager for patching main workflow dependencies."""
    patches = {
        "cleanup": "hbnmigration.from_ripple.to_redcap.cleanup",
        "set_status": "hbnmigration.from_ripple.to_redcap.set_status_in_ripple",
        "push": "hbnmigration.from_ripple.to_redcap.push_to_redcap",
        "prep_ripple": "hbnmigration.from_ripple.to_redcap.prepare_ripple_to_ripple",
        "prep_redcap": "hbnmigration.from_ripple.to_redcap.prepare_redcap_data",
        "request": "hbnmigration.from_ripple.to_redcap.request_potential_participants",
        "vars": "hbnmigration.from_ripple.to_redcap.redcap_variables",
    }
    with ExitStack() as stack:
        yield {name: stack.enter_context(patch(path)) for name, path in patches.items()}


# ============================================================================
# Reusable Patch Context Managers
# ============================================================================


@contextmanager
def patch_redcap_transfer_module(
    fetch_return: Any = None,
    push_return: Any = None,
    update_return: Any = None,
) -> Generator[dict[str, Mock], None, None]:
    """Context manager for patching REDCap transfer module dependencies."""
    with ExitStack() as stack:
        mocks = {
            "fetch": stack.enter_context(
                patch("hbnmigration.from_redcap.to_redcap.fetch_data")
            ),
            "push": stack.enter_context(
                patch("hbnmigration.from_redcap.to_redcap.redcap_api_push")
            ),
            "update": stack.enter_context(
                patch("hbnmigration.from_redcap.to_redcap.update_source")
            ),
            "redcap_vars": stack.enter_context(
                patch("hbnmigration.from_redcap.to_redcap.redcap_variables")
            ),
            "endpoints": stack.enter_context(
                patch("hbnmigration.from_redcap.to_redcap.Endpoints")
            ),
        }
        mocks["redcap_vars"].Tokens.pid744 = "token_744"
        mocks["redcap_vars"].Tokens.pid247 = "token_247"
        mocks["redcap_vars"].headers = {}
        mocks["endpoints"].return_value.base_url = DEFAULT_REDCAP_BASE_URL
        if fetch_return is not None:
            mocks["fetch"].return_value = fetch_return
        if push_return is not None:
            mocks["push"].return_value = push_return
        if update_return is not None:
            mocks["update"].return_value = update_return
        yield mocks


@contextmanager
def patch_redcap_fetch_dependencies(
    fetch_api_return: Any = None,
    endpoints_config: Any = None,
    redcap_vars_config: Any = None,
) -> Generator[dict[str, Mock], None, None]:
    """Context manager for patching fetch_data dependencies."""
    with ExitStack() as stack:
        mocks = {
            "fetch_api": stack.enter_context(
                patch("hbnmigration.from_redcap.from_redcap.fetch_api_data")
            ),
            "endpoints": stack.enter_context(
                patch("hbnmigration.from_redcap.from_redcap.Endpoints")
            ),
            "redcap_vars": stack.enter_context(
                patch("hbnmigration.from_redcap.from_redcap.redcap_variables")
            ),
        }
        if fetch_api_return is not None:
            mocks["fetch_api"].return_value = fetch_api_return
        if endpoints_config is not None:
            mocks["endpoints"].return_value = endpoints_config
        if redcap_vars_config is not None:
            mocks["redcap_vars"] = redcap_vars_config
        yield mocks


@contextmanager
def patch_curious_transfer_module(
    fetch_return: Any = None,
    format_return: Any = None,
    send_return: Any = None,
    update_return: Any = None,
) -> Generator[dict[str, Mock], None, None]:
    """Context manager for patching Curious transfer module dependencies."""
    with ExitStack() as stack:
        mocks = {
            "fetch": stack.enter_context(
                patch("hbnmigration.from_redcap.to_curious.fetch_data")
            ),
            "format": stack.enter_context(
                patch(
                    "hbnmigration.from_redcap.to_curious.format_redcap_data_for_curious"
                )
            ),
            "send": stack.enter_context(
                patch("hbnmigration.from_redcap.to_curious.send_to_curious")
            ),
            "update": stack.enter_context(
                patch("hbnmigration.from_redcap.to_curious.update_redcap")
            ),
            "curious_vars": stack.enter_context(
                patch("hbnmigration.from_redcap.to_curious.curious_variables")
            ),
            "redcap_vars": stack.enter_context(
                patch("hbnmigration.from_redcap.to_curious.redcap_variables")
            ),
        }
        mocks["curious_vars"].applet_ids = {
            "Healthy Brain Network Questionnaires": "test_applet_id"
        }
        mocks["curious_vars"].headers.return_value = {}
        mocks["redcap_vars"].Tokens.pid247 = "token_247"
        mocks["redcap_vars"].headers = {}
        mocks["redcap_vars"].Endpoints.return_value.base_url = DEFAULT_REDCAP_BASE_URL
        if fetch_return is not None:
            mocks["fetch"].return_value = fetch_return
        if format_return is not None:
            mocks["format"].return_value = format_return
        if send_return is not None:
            mocks["send"].return_value = send_return
        if update_return is not None:
            mocks["update"].return_value = update_return
        yield mocks


@contextmanager
def patch_curious_api_dependencies(
    new_account_return: Any = None,
    new_account_side_effect: Any = None,
) -> Generator[dict[str, Mock], None, None]:
    """Context manager for patching Curious API calls."""
    with ExitStack() as stack:
        mocks = {
            "new_account": stack.enter_context(
                patch("hbnmigration.from_redcap.to_curious.new_curious_account")
            ),
            "curious_vars": stack.enter_context(
                patch("hbnmigration.from_redcap.to_curious.curious_variables")
            ),
        }
        token = "test_access_token"
        mocks["curious_vars"].headers.return_value = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        }
        if new_account_return is not None:
            mocks["new_account"].return_value = new_account_return
        if new_account_side_effect is not None:
            mocks["new_account"].side_effect = new_account_side_effect
        yield mocks


# ============================================================================
# Alert Testing Fixtures
# ============================================================================


@pytest.fixture
def sample_curious_alert() -> CuriousAlert:
    """Sample alert from Curious websocket/API."""
    return create_curious_alert(
        "alert_001",
        "00001_P",
        "item_123",
        "Does the child have difficulty concentrating?",
        "respondent_12345",
        "subject_001",
        answerId="answer_456",
    )


@pytest.fixture
def multiple_curious_alerts() -> list[CuriousAlert]:
    """Multiple alerts from different respondents and instruments."""
    return [
        create_curious_alert(
            "alert_001",
            "00001_P",
            "alerts_parent_baseline_1",
            "Parent baseline alert 1",
            "respondent_00001_P",
            "subject_001",
            activityId="parent_baseline",
        ),
        create_curious_alert(
            "alert_002",
            "00001",
            "alerts_child_baseline_1",
            "Child baseline alert 1",
            "respondent_00001",
            "subject_001",
            activityId="child_baseline",
            createdAt="2024-01-01T00:05:00Z",
            answerId="answer_002",
        ),
        create_curious_alert(
            "alert_003",
            "00002_P",
            "alerts_parent_followup_1",
            "Parent followup alert 1",
            "respondent_00002_P",
            "subject_002",
            account_id="account_002",
            activityId="parent_followup",
            createdAt="2024-01-01T00:10:00Z",
            answerId="answer_003",
        ),
    ]


@pytest.fixture
def redcap_alert_df() -> pd.DataFrame:
    """Alerts parsed into REDCap format (before processing)."""
    return pd.DataFrame(
        {
            "record": ["12345", "12345", "67890"],
            "field_name": [
                "alerts_parent_baseline_1",
                "alerts_child_baseline_2",
                "alerts_parent_followup_1",
            ],
            "value": ["Yes", "Sometimes", "No"],
        }
    )


@pytest.fixture
def redcap_alerts_metadata() -> pd.DataFrame:
    """REDCap metadata for alerts instrument."""
    return create_alert_metadata(
        [
            "mrn",
            "alerts_parent_baseline_1",
            "alerts_child_baseline_2",
            "alerts_parent_followup_1",
            "parent_baseline_alerts",
            "child_baseline_alerts",
            "parent_followup_alerts",
        ],
        [
            "",
            "0, No | 1, Yes | 2, Sometimes",
            "0, No | 1, Yes | 2, Sometimes",
            "0, No | 1, Yes",
            "0, No | 1, Yes",
            "0, No | 1, Yes",
            "0, No | 1, Yes",
        ],
    )


@pytest.fixture
def redcap_existing_alert_data() -> pd.DataFrame:
    """Existing REDCap data in EAV format."""
    return create_redcap_eav_df(
        records=["001", "001", "002", "002"],
        field_names=[
            "mrn",
            "alerts_parent_baseline_1",
            "mrn",
            "alerts_parent_followup_1",
        ],
        values=["12345", "0", "67890", ""],
        event_names=[
            "baseline_arm_1",
            "baseline_arm_1",
            "followup_arm_1",
            "followup_arm_1",
        ],
    )


@pytest.fixture
def processed_alerts_for_push() -> pd.DataFrame:
    """Alerts ready for REDCap push."""
    return create_redcap_eav_df(
        records=["001", "001", "001", "002", "002"],
        field_names=[
            "alerts_parent_baseline_1",
            "alerts_child_baseline_2",
            "parent_baseline_alerts",
            "alerts_parent_followup_1",
            "parent_followup_alerts",
        ],
        values=["1", "2", "yes", "0", "yes"],
        event_names=[
            "baseline_arm_1",
            "baseline_arm_1",
            "baseline_arm_1",
            "followup_arm_1",
            "followup_arm_1",
        ],
    )


@pytest.fixture
def kevin_alert() -> CuriousAlert:
    """Alert from Kevin - urgent situation."""
    return create_curious_alert(
        "kevin_alert_001",
        "11111_P",
        "alerts_parent_baseline_5",
        "Urgent: Child showing concerning behavior",
        "respondent_11111_P",
        "subject_005",
        account_id="account_005",
        activityId="parent_baseline",
        createdAt="2024-01-01T12:00:00Z",
        answerId="answer_urgent",
    )


@pytest.fixture
def kevin_redcap_data() -> pd.DataFrame:
    """Kevin's existing REDCap data."""
    return create_alert_df(
        ["005", "005"],
        ["mrn", "alerts_parent_baseline_5"],
        ["11111", ""],
        ["baseline_arm_1", "baseline_arm_1"],
    )


@pytest.fixture
def kevin_metadata() -> pd.DataFrame:
    """Kevin's alert metadata with urgency levels."""
    return create_alert_metadata(
        ["mrn", "alerts_parent_baseline_5", "parent_baseline_alerts"],
        [
            "",
            "0, Normal | 1, Concerning | 2, Urgent",
            "0, No | 1, Yes",
        ],
    )


@pytest.fixture
def sample_curious_alert_response() -> dict[str, Any]:
    """Mock Curious API response with alerts."""
    return {
        "result": [
            create_curious_alert(
                "alert_001",
                "00001_P",
                "alerts_parent_baseline_1",
                "Parent baseline alert",
                "respondent_00001_P",
                "subject_001",
                activityId="parent_baseline",
                answerId="answer_001",
            )
        ]
    }


@pytest.fixture
def mock_alerts_dependencies() -> Generator[dict[str, Mock], None, None]:
    """Mock all external dependencies for alerts processing."""
    with (
        patch(
            "hbnmigration.from_curious.alerts_to_redcap.fetch_api_data"
        ) as mock_fetch_api,
        patch("hbnmigration.from_curious.alerts_to_redcap.fetch_data") as mock_fetch,
        patch(
            "hbnmigration.from_curious.alerts_to_redcap.redcap_api_push"
        ) as mock_push,
        patch(
            "hbnmigration.from_curious.alerts_to_redcap.redcap_variables"
        ) as mock_vars,
        patch(
            "hbnmigration.from_curious.alerts_to_redcap._fetch_alerts_metadata"
        ) as mock_fetch_metadata,
        patch(
            "hbnmigration.from_curious.alerts_to_redcap._create_choice_lookup"
        ) as mock_choice_lookup,
    ):
        mock_vars.Tokens.pid625 = "token_625"
        mock_vars.headers = {}
        mock_vars.Endpoints.return_value.base_url = DEFAULT_REDCAP_BASE_URL
        mock_choice_lookup.return_value = {}
        yield {
            "fetch_api": mock_fetch_api,
            "fetch": mock_fetch,
            "push": mock_push,
            "vars": mock_vars,
            "fetch_metadata": mock_fetch_metadata,
            "choice_lookup": mock_choice_lookup,
        }


# ============================================================================
# WebSocket / Reconnection Testing Utilities
# ============================================================================


def create_mock_tokens_ws(auth_token: str = "test_token") -> Mock:
    """Create mock authentication tokens for WebSocket connections."""
    mock_tokens = Mock()
    mock_tokens.access = auth_token
    mock_tokens.endpoints = Mock()
    mock_tokens.endpoints.alerts = "wss://curious.test/alerts"
    return mock_tokens


def create_mock_invalid_status(
    status_code: int = requests.codes["unauthorized"],
) -> InvalidStatus:
    """Create mock InvalidStatus exception for testing auth failures."""
    mock_response = Mock()
    mock_response.status_code = status_code
    exc = InvalidStatus(mock_response)
    exc.response = mock_response
    return exc


def setup_standard_alert_mocks(
    mock_alerts_dependencies: dict[str, Mock],
    metadata: pd.DataFrame | None = None,
    existing_data: pd.DataFrame | None = None,
) -> None:
    """Set up standard mock returns for alert processing tests."""
    mocks = mock_alerts_dependencies
    mocks["fetch_metadata"].return_value = (
        metadata
        if metadata is not None
        else create_alert_metadata(
            ["mrn", "alerts_parent_baseline_1"],
            ["", "0, No | 1, Yes"],
        )
    )
    mocks["fetch"].return_value = (
        existing_data
        if existing_data is not None
        else create_alert_df(
            ["1", "1"],
            ["mrn", "alerts_parent_baseline_1"],
            ["12345", "0"],
            ["baseline_arm_1", "baseline_arm_1"],
        )
    )


@pytest.fixture
def multi_instrument_alert_df() -> pd.DataFrame:
    """Create alert DataFrame with multiple instruments for testing."""
    return create_alert_df(
        ["12345", "12345", "67890"],
        [
            "alerts_parent_baseline_1",
            "alerts_child_baseline_2",
            "alerts_parent_followup_1",
        ],
        ["Yes", "Sometimes", "No"],
        ["baseline_arm_1", "baseline_arm_1", "followup_arm_1"],
    )


# ============================================================================
# Context Managers for WebSocket / Reconnection Tests
# ============================================================================


@contextmanager
def setup_reconnect_mocks(
    listener_side_effect: Any = None,
) -> Generator[dict[str, Mock], None, None]:
    """Set up mocks for main_with_reconnect tests."""
    with ExitStack() as stack:
        mocks = {
            "ws": stack.enter_context(
                patch("hbnmigration.from_curious.alerts_to_redcap.connect_to_websocket")
            ),
            "listener": stack.enter_context(
                patch(
                    "hbnmigration.from_curious.alerts_to_redcap.websocket_listener",
                    new_callable=AsyncMock,
                )
            ),
            "sleep": stack.enter_context(
                patch(
                    "hbnmigration.from_curious.alerts_to_redcap.asyncio.sleep",
                    new_callable=AsyncMock,
                )
            ),
        }
        mock_websocket = AsyncMock()
        mocks["ws"].return_value.__aenter__.return_value = mock_websocket
        if listener_side_effect is not None:
            mocks["listener"].side_effect = listener_side_effect
        yield mocks


@contextmanager
def setup_main_test_mocks(
    mock_alerts_dependencies: dict[str, Mock],
    sample_alert: dict[str, Any] | None = None,
    parse_return: Any = None,
    metadata_return: pd.DataFrame | None = None,
) -> Generator[dict[str, Mock], None, None]:
    """Context manager for common async main test setup."""
    setup_standard_alert_mocks(
        mock_alerts_dependencies,
        metadata_return
        if metadata_return is not None
        else create_alert_metadata(
            ["mrn", "alerts_parent_baseline_1"],
            ["", "0, No | 1, Yes"],
        ),
    )
    with ExitStack() as stack:
        mocks = {
            "auth": stack.enter_context(
                patch("hbnmigration.from_curious.alerts_to_redcap.curious_authenticate")
            ),
            "ws": stack.enter_context(
                patch("hbnmigration.from_curious.alerts_to_redcap.connect_to_websocket")
            ),
            "parse": stack.enter_context(
                patch("hbnmigration.from_curious.alerts_to_redcap.parse_alert")
            ),
        }
        mocks["auth"].return_value = create_mock_tokens_ws()
        mock_websocket = AsyncMock()
        if sample_alert is not None:
            mock_websocket.__aiter__.return_value = [json.dumps(sample_alert)]
        mocks["ws"].return_value.__aenter__.return_value = mock_websocket
        if parse_return is not None:
            mocks["parse"].return_value = parse_return
        yield mocks


@contextmanager
def setup_sync_main_mocks(
    mock_alerts_dependencies: dict[str, Mock],
    alerts_list: list[Any],
    parse_returns: Any,
    metadata_return: pd.DataFrame | None = None,
    status_code: int = requests.codes["okay"],
) -> Generator[dict[str, Mock], None, None]:
    """Set up synchronous main test mocks."""
    setup_standard_alert_mocks(
        mock_alerts_dependencies,
        metadata_return
        if metadata_return is not None
        else create_alert_metadata(
            ["mrn", "alerts_parent_baseline_1"],
            ["", "0, No | 1, Yes"],
        ),
    )
    with ExitStack() as stack:
        mocks = {
            "auth": stack.enter_context(
                patch("hbnmigration.from_curious.alerts_to_redcap.curious_authenticate")
            ),
            "get": stack.enter_context(
                patch("hbnmigration.from_curious.alerts_to_redcap.requests.get")
            ),
            "parse": stack.enter_context(
                patch("hbnmigration.from_curious.alerts_to_redcap.parse_alert")
            ),
            "curious_vars": stack.enter_context(
                patch("hbnmigration.from_curious.alerts_to_redcap.curious_variables")
            ),
        }
        mocks["auth"].return_value = create_mock_tokens_ws()
        mock_response = Mock()
        mock_response.status_code = status_code
        if status_code == requests.codes["okay"]:
            mock_response.json.return_value = {"result": alerts_list}
        mocks["get"].return_value = mock_response
        if parse_returns is not None:
            mocks["parse"].side_effect = parse_returns
        mocks["curious_vars"].headers.return_value = {}
        yield mocks


@contextmanager
def setup_cli_mocks(
    **patches: Any,
) -> Generator[dict[str, Mock], None, None]:
    """Set up CLI test mocks with specified patches."""
    with ExitStack() as stack:
        yield {
            "run": stack.enter_context(
                patch("hbnmigration.from_curious.alerts_to_redcap.asyncio.run")
            ),
            "sync": stack.enter_context(
                patch("hbnmigration.from_curious.alerts_to_redcap.synchronous_main")
            ),
            "main": stack.enter_context(
                patch("hbnmigration.from_curious.alerts_to_redcap.main")
            ),
            "argv": stack.enter_context(
                patch(
                    "sys.argv",
                    patches.get("argv", ["alerts_to_redcap.py"]),
                )
            ),
        }


# ============================================================================
# Helper Functions
# ============================================================================


def setup_curious_integration_mocks(
    curious_vars_mock: Mock, redcap_vars_mock: Mock
) -> None:
    """Set up common mock configurations for integration tests."""
    redcap_vars_mock.Tokens.pid247 = "token_247"
    redcap_vars_mock.headers = {}
    redcap_vars_mock.Endpoints.return_value.base_url = DEFAULT_REDCAP_BASE_URL
    curious_vars_mock.applet_ids = {
        "Healthy Brain Network Questionnaires": "test_applet"
    }
    curious_tokens = Mock()
    curious_tokens.access, curious_tokens.endpoints.base_url = (
        "test_token",
        "https://curious.test/",
    )
    curious_vars_mock.Tokens.return_value = curious_tokens
    curious_vars_mock.headers.return_value = {}


def create_curious_api_failure(
    message: str = "API Error",
) -> requests.exceptions.RequestException:
    """Create a RequestException for testing."""
    return requests.exceptions.RequestException(message)


@contextmanager
def create_mock_module_in_sys(
    module_path: str, attributes: dict[str, Any] | None = None
) -> Generator[Mock, None, None]:
    """Return context manager to temporarily add mock module to sys.modules."""
    mock_mod = Mock()
    if attributes:
        for key, value in attributes.items():
            setattr(mock_mod, key, value)
    with patch.dict("sys.modules", {module_path: mock_mod}):
        yield mock_mod


# ============================================================================
# Assertion helpers
# ============================================================================


def assert_valid_redcap_columns(result_df: pd.DataFrame) -> None:
    """Assert DataFrame has valid REDCap columns."""
    assert "record_id" in result_df.columns and "mrn" in result_df.columns
    assert result_df["record_id"].iloc[0] is not None


def assert_valid_email_extraction(result_df: pd.DataFrame, expected_email: str) -> None:
    """Assert email was properly extracted."""
    assert "email_consent" in result_df.columns
    assert result_df["email_consent"].iloc[0] == expected_email


def assert_cleanup_called(mock_cleanup: Mock) -> None:
    """Assert cleanup was called exactly once."""
    mock_cleanup.assert_called_once()


def assert_is_fallback_value(result: Any, expected_fallback: Any) -> None:
    """Assert that result matches the expected fallback value."""
    assert result == expected_fallback


def assert_not_fallback_value(result: Any, fallback: Any) -> None:
    """Assert that result is NOT the fallback (successful import)."""
    assert result != fallback


def assert_is_callable_result(result: Any) -> None:
    """Assert result is callable (function/method)."""
    assert callable(result)


def assert_has_name_attribute(result: Any, expected_name: str) -> None:
    """Assert result has __name__ attribute with expected value."""
    assert hasattr(result, "__name__") and result.__name__ == expected_name


def assert_redcap_eav_structure(df: pd.DataFrame) -> None:
    """Assert DataFrame has valid REDCap EAV structure."""
    for col in ["record", "field_name", "value"]:
        assert col in df.columns, f"Missing required column: {col}"


def assert_field_renamed(df: pd.DataFrame, old_name: str, new_name: str) -> None:
    """Assert field was renamed correctly."""
    assert (
        new_name in df["field_name"].values and old_name not in df["field_name"].values
    )


def assert_permission_decremented(
    df: pd.DataFrame, original_value: str, expected_value: str
) -> None:
    """Assert permission_collab was decremented correctly."""
    perm_row = df[df["field_name"] == "permission_collab"]
    assert len(perm_row) > 0
    actual = str(perm_row["value"].iloc[0]).rstrip(".0")
    expected = expected_value.rstrip(".0")
    assert actual == expected


def count_records_in_eav(df: pd.DataFrame) -> int:
    """Count unique records in EAV DataFrame."""
    return len(df["record"].unique())


def count_fields_per_record(df: pd.DataFrame) -> int:
    """Count unique field names in EAV DataFrame."""
    return len(df["field_name"].unique())


def calculate_total_eav_rows(df: pd.DataFrame) -> int:
    """Calculate total rows in EAV format."""
    return count_records_in_eav(df) * count_fields_per_record(df)


def get_field_values(df: pd.DataFrame, field_name: str) -> pd.Series:
    """Extract values for a specific field from EAV DataFrame."""
    return df[df["field_name"] == field_name]["value"]


def get_unique_field_values(df: pd.DataFrame, field_name: str) -> list[Any]:
    """Get unique values for a specific field from EAV DataFrame."""
    return sorted(df[df["field_name"] == field_name]["value"].unique())


def assert_valid_curious_format(df: pd.DataFrame) -> None:
    """Assert DataFrame has valid Curious format."""
    for col in ["secretUserId", "accountType"]:
        assert col in df.columns


def assert_secret_user_id_format(df: pd.DataFrame, expected_length: int = 5) -> None:
    """Assert secretUserId is properly formatted."""
    for user_id in df["secretUserId"]:
        base_id = user_id.rstrip("_P")
        assert len(base_id) == expected_length and (
            base_id.isdigit() or base_id.isalnum()
        )


def assert_parent_suffix(df: pd.DataFrame) -> None:
    """Assert parent records have _P suffix."""
    parent_rows = df[df["tag"] == "parent"]
    if len(parent_rows) > 0:
        assert all(parent_rows["secretUserId"].str.endswith("_P"))


def assert_no_parent_suffix(df: pd.DataFrame) -> None:
    """Assert child records do not have _P suffix."""
    child_rows = df[df["tag"] == "child"]
    if len(child_rows) > 0:
        assert all(~child_rows["secretUserId"].str.endswith("_P"))


def assert_parent_account_type(df: pd.DataFrame) -> None:
    """Assert parent records have 'full' account type."""
    parent_rows = df[df["tag"] == "parent"]
    if len(parent_rows) > 0:
        assert all(parent_rows["accountType"] == "full")


def assert_child_account_type(df: pd.DataFrame) -> None:
    """Assert child records have 'limited' account type."""
    child_rows = df[df["tag"] == "child"]
    if len(child_rows) > 0:
        assert all(child_rows["accountType"] == "limited")


def get_curious_records_by_type(df: pd.DataFrame, account_type: str) -> pd.DataFrame:
    """Get records filtered by account type."""
    return df[df["accountType"] == account_type]


def get_curious_records_by_tag(df: pd.DataFrame, tag: str) -> pd.DataFrame:
    """Get records filtered by tag (parent/child)."""
    return df[df["tag"] == tag]


def count_curious_accounts(df: pd.DataFrame) -> dict[str, int]:
    """Count accounts by tag (parent/child)."""
    return {
        "parent": len(df[df["tag"] == "parent"]),
        "child": len(df[df["tag"] == "child"]),
        "total": len(df),
    }


def assert_no_none_in_records(records: list[dict[str, Any]]) -> None:
    """Assert that no None values exist in record dictionaries."""
    for record in records:
        for _key, value in record.items():
            assert value is not None


def assert_enrollment_complete_updated(df: pd.DataFrame, expected_value: str) -> None:
    """Assert enrollment_complete field has expected value."""
    enrollment_rows = df[df["field_name"] == "enrollment_complete"]
    assert len(enrollment_rows) > 0 and all(enrollment_rows["value"] == expected_value)


def assert_alert_summary_toggled(df: pd.DataFrame, instrument: str) -> None:
    """Assert that alert summary flag is properly toggled."""
    summary_field = f"{instrument}_alerts"
    assert summary_field in df["field_name"].values
    assert all(df[df["field_name"] == summary_field]["value"] == "yes")


def assert_response_mapped_to_index(
    df: pd.DataFrame,
    field: str,
    response: str,
    expected_index: int,
) -> None:
    """Assert that response value was mapped to correct index."""
    rows = df[df["field_name"] == field]
    assert len(rows) > 0 and str(expected_index) in rows["value"].values


def assert_alert_has_event_name(df: pd.DataFrame) -> None:
    """Assert that all alert rows have redcap_event_name."""
    assert "redcap_event_name" in df.columns and all(df["redcap_event_name"].notna())


def assert_mrn_mapped_to_record(
    df: pd.DataFrame, mrn: str, expected_record: str
) -> None:
    """Assert that MRN was correctly mapped to record ID."""
    assert expected_record in df["record"].values and mrn not in df["record"].values


def count_alert_summary_rows(df: pd.DataFrame) -> int:
    """Count instrument-level alert summary rows."""
    return len(df[df["field_name"].str.endswith("_alerts")])


def get_alert_values_by_instrument(df: pd.DataFrame, instrument: str) -> pd.DataFrame:
    """Get all alert values for a specific instrument."""
    return df[df["field_name"].str.match(f"alerts_{instrument}_.*")]
