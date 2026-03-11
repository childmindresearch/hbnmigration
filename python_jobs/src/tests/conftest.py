"""Shared pytest configuration and fixtures."""

from contextlib import contextmanager, ExitStack
from pathlib import Path
import tempfile
from typing import Any, Dict, List, Optional, TypedDict
from unittest.mock import Mock, patch

import pandas as pd
import pytest
import requests

from hbnmigration.from_redcap.config import Values

# ============================================================================
# File System Fixtures
# ============================================================================


@pytest.fixture
def temp_csv_file():
    """Create a temporary CSV file."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp:
        tmp_path = tmp.name
    yield Path(tmp_path)
    Path(tmp_path).unlink(missing_ok=True)


@pytest.fixture
def temp_excel_file():
    """Create a temporary Excel file."""
    with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
        tmp_path = tmp.name
    yield Path(tmp_path)
    Path(tmp_path).unlink(missing_ok=True)


@pytest.fixture
def temp_dir():
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
def mock_redcap_response():
    """Mock successful REDCap API response."""
    return _create_mock_response(200, "1")


@pytest.fixture
def mock_ripple_response():
    """Mock successful Ripple API response."""
    return _create_mock_response(200, "Success")


# ============================================================================
# Participant Data Fixtures - Base Factory
# ============================================================================


def create_participant_df(  # noqa: PLR0913
    global_ids: Optional[List[str]] = None,
    custom_ids: Optional[List[int]] = None,
    first_names: Optional[List[str]] = None,
    last_names: Optional[List[str]] = None,
    consent_forms: Optional[List[str]] = None,
    contact_types: Optional[List[str]] = None,
    contact_info: Optional[List[str]] = None,
    import_types: Optional[List[str]] = None,
    **kwargs,
) -> pd.DataFrame:
    """
    Create participant DataFrames with flexible defaults.

    Parameters
    ----------
    global_ids
        List of global IDs
    custom_ids
        List of custom IDs
    first_names
        List of first names
    last_names
        List of last names
    consent_forms
        List of consent form statuses
    contact_types
        List of contact types
    contact_info
        List of contact information
    import_types
        List of import types
    **kwargs
        Additional columns to add

    Returns
    -------
    pd.DataFrame
        Participant data

    """
    # Determine length from first provided list or default to 1
    length = len(global_ids or custom_ids or first_names or [1])
    data = {
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
    # Add any additional columns
    data.update(kwargs)
    return pd.DataFrame(data)


def create_redcap_eav_df(
    records: Optional[List[str]] = None,
    field_names: Optional[List[str]] = None,
    values: Optional[List[str]] = None,
    repeat_instruments: Optional[List[str]] = None,
    repeat_instances: Optional[List[Any]] = None,
) -> pd.DataFrame:
    """
    Create REDCap EAV format DataFrames with flexible defaults.

    Parameters
    ----------
    records
        List of record IDs
    field_names
        List of field names
    values
        List of values
    repeat_instruments
        List of repeat instrument names
    repeat_instances
        List of repeat instance numbers

    Returns
    -------
    pd.DataFrame
        REDCap EAV formatted data

    """
    # Handle empty case
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
    return pd.DataFrame(
        {
            "record": records or [""] * length,
            "field_name": field_names or [""] * length,
            "value": values or [""] * length,
            "redcap_repeat_instrument": repeat_instruments or [""] * length,
            "redcap_repeat_instance": repeat_instances or [""] * length,
        }
    )


def create_curious_participant_df(
    secret_user_ids: Optional[List[str]] = None,
    tags: Optional[List[str]] = None,  # parent/child
    first_names: Optional[List[str]] = None,
    last_names: Optional[List[str]] = None,
    **kwargs,
) -> pd.DataFrame:
    """
    Create Curious participant DataFrames with flexible defaults.

    Parameters
    ----------
    secret_user_ids
        List of secret user IDs
    tags
        List of tags (parent/child)
    first_names
        List of first names
    last_names
        List of last names
    **kwargs
        Additional columns to add

    Returns
    -------
    pd.DataFrame
        Curious participant data

    """
    length = len(secret_user_ids or tags or first_names or [1])
    default_tags = tags or ["child"] * length

    # Map tags to account types
    account_types_list = ["full" if t == "parent" else "limited" for t in default_tags]

    data = {
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
# Participant Data Fixtures
# ============================================================================


@pytest.fixture
def participant_with_email():
    """Return generic participant with email contact."""
    return create_participant_df(
        global_ids=["TEST001"],
        custom_ids=[12345],
        first_names=["Test"],
        contact_types=["email"],
        contact_info=["test@swamp.com"],
    )


@pytest.fixture
def participant_without_email():
    """Return generic participant without email contact."""
    return create_participant_df(
        global_ids=["TEST002"],
        custom_ids=[67890],
        first_names=["NoEmail"],
        contact_types=["phone"],
        contact_info=["555-0123"],
    )


@pytest.fixture
def send_to_redcap_participant():
    """Return participant with 'Send to RedCap' consent status."""
    return create_participant_df(
        global_ids=["TEST003"],
        custom_ids=[99999],
        first_names=["Ready"],
        consent_forms=["Send to RedCap"],
    )


@pytest.fixture
def swamp_thing_participant():
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
def parliament_of_trees_participants():
    """Provide multiple Parliament of Trees members."""
    return create_participant_df(
        global_ids=["ST001", "AA001", "TE001"],
        custom_ids=[12345, 67890, 11111],
        first_names=["Alec", "Abby", "Tefé"],
        last_names=["Holland", "Arcane", "Holland"],
        contact_info=["alec@swamp.com", "abby@parliament.org", "tefe@green.org"],
        import_types=["HBN - Main", "HBN - Main", "HBN - Waitlist"],
    )


@pytest.fixture
def sample_ripple_data():
    """Return Ripple data with multiple participants - Alec Holland & Abby Arcane."""
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
        import_types=["HBN - Main", "HBN - Waitlist", "HBN - Main", "HBN - Waitlist"],
    )


@pytest.fixture
def anton_arcane_corrupted_data():
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
def mock_redcap_existing_subjects():
    """Mock existing REDCap subjects - Alec and Abby already in system."""
    return pd.DataFrame(
        {
            "mrn": [12345, 67890],
            "record_id": [1, 2],
        }
    )


@pytest.fixture
def incoming_subjects_mixed():
    """Return subjects with mix of new and existing."""
    return pd.DataFrame(
        {
            "record_id": [999, 998, 997],
            "mrn": [12345, 67890, 99001],  # First two exist, last is new
            "email_consent": [
                "alec@swamp.com",
                "abby@parliament.org",
                "bella@garden.green",
            ],
        }
    )


@pytest.fixture
def bella_garten_participant():
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
def sample_redcap_data():
    """Sample REDCap data in EAV format from PID 247."""
    return create_redcap_eav_df(
        records=["001", "001", "001", "002", "002", "002"],
        field_names=[
            "intake_ready",
            "participant_name",
            "permission_collab",
            "intake_ready",
            "participant_name",
            "permission_collab",
        ],
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
def empty_redcap_data():
    """Empty DataFrame representing no data from REDCap."""
    return create_redcap_eav_df()


@pytest.fixture
def expected_transformed_data():
    """Return expected data after transformation for PID 744."""
    return pd.DataFrame(
        {
            "record": ["001", "001", "002", "002"],
            "field_name": [
                "participant_full_name",
                "permission_collab",
                "participant_full_name",
                "permission_collab",
            ],
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
def sample_redcap_curious_data():
    """Sample REDCap data ready for Curious transfer with proper field mapping."""
    return create_redcap_eav_df(
        records=["001", "001", "001", "001", "001"],
        field_names=[
            "mrn",
            "enrollment_complete",
            "consent_parent_first_name",
            "consent_child_first_name",
            "parent_involvement___1",
        ],
        values=[
            "12345",
            "1",
            "Alec",
            "Tefé",
            "1",
        ],
    )


@pytest.fixture
def parliament_curious_redcap_data():
    """Parliament of Trees REDCap data for Curious transfer."""
    return create_redcap_eav_df(
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


@pytest.fixture
def formatted_curious_data():
    """Sample formatted data ready for Curious API."""
    return pd.DataFrame(
        {
            "secretUserId": ["00001", "00001_P"],
            "accountType": ["limited", "full"],  # child=limited, parent=full
            "firstName": ["Tefé", "Alec"],
            "lastName": ["Holland", "Holland"],
            "nickname": [None, None],
            "role": ["respondent", "respondent"],
            "tag": ["child", "parent"],
            "language": ["en", "en"],
        }
    )


@pytest.fixture
def multi_record_curious_data():
    """Multiple records formatted for Curious."""
    return pd.DataFrame(
        {
            "secretUserId": ["00001", "00001_P", "00002", "00002_P"],
            "accountType": ["limited", "full", "limited", "full"],
            "firstName": ["Tefé", "Alec", "Constantine", "Abby"],
            "lastName": ["Holland", "Holland", "Arcane", "Arcane"],
            "nickname": [None, None, None, None],
            "role": ["respondent"] * 4,
            "tag": ["child", "parent", "child", "parent"],
            "language": ["en"] * 4,
        }
    )


# ============================================================================
# Curious/MindLogger Fixtures
# ============================================================================


@pytest.fixture
def mock_curious_variables():
    """Mock curious_variables configuration."""
    mock_vars = Mock()
    mock_vars.headers = {"Content-Type": "application/json"}
    mock_vars.applet_ids = {"Healthy Brain Network Questionnaires": "test_applet_id"}

    # Mock Credentials
    mock_creds = Mock()
    mock_creds.hbn_mindlogger = Mock(username="test_user", password="test_pass")
    mock_vars.Credentials = mock_creds

    # Mock Tokens
    mock_tokens = Mock()
    mock_tokens.access = "test_access_token"
    mock_tokens.endpoints = Mock()
    mock_tokens.endpoints.base_url = "https://curious.test/api/"
    mock_vars.Tokens = Mock(return_value=mock_tokens)

    # Mock Endpoints
    mock_endpoints = Mock()
    mock_endpoints.base_url = "https://curious.test/api/"
    mock_vars.Endpoints = Mock(return_value=mock_endpoints)

    return mock_vars


@pytest.fixture
def mock_redcap_variables_curious():
    """Mock redcap_variables for Curious transfer."""
    mock_vars = Mock()
    mock_vars.Tokens.pid247 = "token_247"
    mock_vars.headers = {"Content-Type": "application/x-www-form-urlencoded"}
    mock_vars.Endpoints = Mock()
    mock_vars.Endpoints.return_value.base_url = "https://redcap.test/api/"
    return mock_vars


# ============================================================================
# Mock Configuration Fixtures
# ============================================================================


@pytest.fixture
def mock_ripple_variables():
    """Mock ripple_variables configuration with common defaults."""
    mock_vars = Mock()
    mock_vars.study_ids = {
        "HBN - Main": "main_study_id",
        "HBN - Waitlist": "waitlist_study_id",
    }
    mock_vars.column_dict.return_value = {}
    mock_vars.headers = {"import": {"Content-Type": "application/octet-stream"}}
    return mock_vars


@pytest.fixture
def mock_redcap_variables():
    """Mock redcap_variables configuration."""
    mock_vars = Mock()
    mock_vars.Tokens.pid757 = "dev_token"
    mock_vars.Tokens.pid247 = "prod_token"
    mock_vars.Tokens.pid744 = "token_744"
    mock_vars.headers = {"Content-Type": "application/x-www-form-urlencoded"}
    return mock_vars


@pytest.fixture
def setup_redcap_mocks(mock_redcap_variables, temp_csv_file):
    """Set up common redcap variable mocks with temp file."""
    mock_redcap_variables.redcap_import_file = temp_csv_file
    return mock_redcap_variables


@pytest.fixture
def mock_endpoints():
    """Mock Endpoints configuration."""
    mock = Mock()
    mock.Ripple.import_data.return_value = "https://ripple.swamp.org/import"
    mock.Ripple.export_from_ripple.return_value = pd.DataFrame()
    mock.REDCap.base_url = "https://redcap.swamp.org/api/"
    return mock


@pytest.fixture
def mock_all_ripple_deps(mock_ripple_variables, mock_endpoints):
    """Set up all common Ripple dependencies with Endpoints."""
    return {
        "endpoints": mock_endpoints,
        "variables": mock_ripple_variables,
    }


@pytest.fixture
def mock_main_workflow_deps(mock_redcap_variables, temp_excel_file):
    """Set up dependencies for main workflow tests."""
    return {
        "vars": mock_redcap_variables,
        "excel_file": temp_excel_file,
    }


# ============================================================================
# Excel File Fixtures
# ============================================================================


@pytest.fixture
def excel_file_with_data(temp_excel_file):
    """Create Excel file with test data - Swamp Thing consent completed."""
    test_df = pd.DataFrame(
        {
            "globalId": ["ST001"],
            "cv.consent_form": ["consent_form_created_in_redcap"],
        }
    )
    test_df.to_excel(temp_excel_file, index=False)
    return temp_excel_file


# ============================================================================
# Import Testing Fixtures
# ============================================================================


@pytest.fixture
def mock_importable_module():
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
def green_realm_config():
    """Mock configuration data for API testing."""
    return {
        "api_key": "TEST_KEY",
        "endpoint": "https://green.realm/api",
        "timeout": 30,
        "retry_attempts": 3,
    }


@pytest.fixture
def mock_parliament_object():
    """Mock Parliament of Trees object for testing."""
    mock_parliament = Mock()
    mock_parliament.members = ["Alec Holland", "Ghost Orchid", "Yggdrasil"]
    mock_parliament.collective_consciousness = True
    mock_parliament.green_connection = Mock()
    return mock_parliament


# ============================================================================
# Workflow Patching Fixtures
# ============================================================================


@pytest.fixture
def patched_main_workflow():
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
        mocks = {
            name: stack.enter_context(patch(path)) for name, path in patches.items()
        }
        yield mocks


# ============================================================================
# Reusable Patch Context Managers
# ============================================================================


@contextmanager
def patch_redcap_transfer_module(
    fetch_return=None,
    push_return=None,
    update_return=None,
):
    """
    Context manager for patching REDCap transfer module dependencies.

    Uses real Fields configuration from config module.

    Parameters
    ----------
    fetch_return
        Return value for fetch_data
    push_return
        Return value for redcap_api_push
    update_return
        Return value for update_source

    Yields
    ------
    dict
        Dictionary of mocked objects

    """
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
        # Set up basic configurations
        mocks["redcap_vars"].Tokens.pid744 = "token_744"
        mocks["redcap_vars"].Tokens.pid247 = "token_247"
        mocks["redcap_vars"].headers = {}
        mocks["endpoints"].return_value.base_url = "https://redcap.test/api/"
        if fetch_return is not None:
            mocks["fetch"].return_value = fetch_return
        if push_return is not None:
            mocks["push"].return_value = push_return
        if update_return is not None:
            mocks["update"].return_value = update_return
        yield mocks


@contextmanager
def patch_redcap_fetch_dependencies(
    fetch_api_return=None,
    endpoints_config=None,
    redcap_vars_config=None,
):
    """
    Context manager for patching fetch_data dependencies.

    Uses real Fields configuration from config module.

    Parameters
    ----------
    fetch_api_return
        Return value for fetch_api_data
    endpoints_config
        Mock Endpoints configuration
    redcap_vars_config
        Mock redcap_variables configuration

    Yields
    ------
    dict
        Dictionary of mocked objects

    """
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
    fetch_return=None,
    format_return=None,
    send_return=None,
    update_return=None,
):
    """
    Context manager for patching Curious transfer module dependencies.

    Parameters
    ----------
    fetch_return
        Return value for fetch_data
    format_return
        Return value for format_redcap_data_for_curious
    send_return
        Return value for send_to_curious (list of failures)
    update_return
        Return value for update_redcap

    Yields
    ------
    dict
        Dictionary of mocked objects

    """
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

        # Set up basic configurations
        mocks["curious_vars"].applet_ids = {
            "Healthy Brain Network Questionnaires": "test_applet_id"
        }
        mocks["redcap_vars"].Tokens.pid247 = "token_247"
        mocks["redcap_vars"].headers = {}
        mocks[
            "redcap_vars"
        ].Endpoints.return_value.base_url = "https://redcap.test/api/"

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
    new_account_return=None,
    new_account_side_effect=None,
):
    """
    Context manager for patching Curious API calls.

    Parameters
    ----------
    new_account_return
        Return value for new_curious_account
    new_account_side_effect
        Side effect for new_curious_account (for testing multiple calls)

    Yields
    ------
    dict
        Dictionary of mocked objects

    """
    with ExitStack() as stack:
        mocks = {
            "new_account": stack.enter_context(
                patch("hbnmigration.from_redcap.to_curious.new_curious_account")
            ),
        }

        if new_account_return is not None:
            mocks["new_account"].return_value = new_account_return
        if new_account_side_effect is not None:
            mocks["new_account"].side_effect = new_account_side_effect

        yield mocks


# ============================================================================
# Helper Functions - Assertions
# ============================================================================


def assert_valid_redcap_columns(result_df: pd.DataFrame) -> None:
    """Assert DataFrame has valid REDCap columns."""
    assert "record_id" in result_df.columns
    assert "mrn" in result_df.columns
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
    assert hasattr(result, "__name__")
    assert result.__name__ == expected_name


def assert_redcap_eav_structure(df: pd.DataFrame) -> None:
    """Assert DataFrame has valid REDCap EAV structure."""
    required_columns = ["record", "field_name", "value"]
    for col in required_columns:
        assert col in df.columns, f"Missing required column: {col}"


def assert_field_renamed(df: pd.DataFrame, old_name: str, new_name: str) -> None:
    """Assert field was renamed correctly."""
    assert new_name in df["field_name"].values, (
        f"Field '{new_name}' not found. Available fields: {df['field_name'].unique()}"
    )
    assert old_name not in df["field_name"].values, (
        f"Old field name '{old_name}' still present in DataFrame"
    )


def assert_permission_decremented(
    df: pd.DataFrame,
    original_value: str,
    expected_value: str,
) -> None:
    """Assert permission_collab was decremented correctly."""
    perm_row = df[df["field_name"] == "permission_collab"]
    assert len(perm_row) > 0, "No permission_collab field found in DataFrame"
    actual_value = str(perm_row["value"].iloc[0])
    expected_normalized = (
        expected_value.rstrip(".0") if "." in expected_value else expected_value
    )
    actual_normalized = (
        actual_value.rstrip(".0") if "." in actual_value else actual_value
    )
    assert actual_normalized == expected_normalized, (
        f"Expected permission_collab value '{expected_value}', got '{actual_value}'"
    )


def count_records_in_eav(df: pd.DataFrame) -> int:
    """Count unique records in EAV DataFrame."""
    return len(df["record"].unique())


def count_fields_per_record(df: pd.DataFrame) -> int:
    """Count unique field names in EAV DataFrame."""
    return len(df["field_name"].unique())


def calculate_total_eav_rows(df: pd.DataFrame) -> int:
    """Calculate total rows in EAV format (records × fields)."""  # noqa: RUF002
    return count_records_in_eav(df) * count_fields_per_record(df)


def get_field_values(df: pd.DataFrame, field_name: str) -> pd.Series:
    """Extract values for a specific field from EAV DataFrame."""
    return df[df["field_name"] == field_name]["value"]


def get_unique_field_values(df: pd.DataFrame, field_name: str) -> list:
    """Get unique values for a specific field from EAV DataFrame."""
    return sorted(df[df["field_name"] == field_name]["value"].unique())


# ============================================================================
# Helper Functions - Curious Data Assertions
# ============================================================================


def assert_valid_curious_format(df: pd.DataFrame) -> None:
    """Assert DataFrame has valid Curious format."""
    required_columns = ["secretUserId", "accountType"]
    for col in required_columns:
        assert col in df.columns, f"Missing required column: {col}"


def assert_secret_user_id_format(df: pd.DataFrame, expected_length: int = 5) -> None:
    """Assert secretUserId is properly formatted."""
    for user_id in df["secretUserId"]:
        base_id = user_id.rstrip("_P")
        assert len(base_id) == expected_length, (
            f"secretUserId '{base_id}' should be {expected_length} characters"
        )
        assert base_id.isdigit() or base_id.isalnum(), (
            f"secretUserId '{base_id}' should be numeric or alphanumeric"
        )


def assert_parent_suffix(df: pd.DataFrame) -> None:
    """Assert parent records have _P suffix."""
    parent_rows = df[df["tag"] == "parent"]
    if len(parent_rows) > 0:
        assert all(parent_rows["secretUserId"].str.endswith("_P")), (
            "All parent records should have _P suffix"
        )


def assert_no_parent_suffix(df: pd.DataFrame) -> None:
    """Assert child records do not have _P suffix."""
    child_rows = df[df["tag"] == "child"]
    if len(child_rows) > 0:
        assert all(~child_rows["secretUserId"].str.endswith("_P")), (
            "Child records should not have _P suffix"
        )


def assert_parent_account_type(df: pd.DataFrame) -> None:
    """Assert parent records have 'full' account type."""
    parent_rows = df[df["tag"] == "parent"]
    if len(parent_rows) > 0:
        assert all(parent_rows["accountType"] == "full"), (
            "All parent records should have accountType='full'"
        )


def assert_child_account_type(df: pd.DataFrame) -> None:
    """Assert child records have 'limited' account type."""
    child_rows = df[df["tag"] == "child"]
    if len(child_rows) > 0:
        assert all(child_rows["accountType"] == "limited"), (
            "All child records should have accountType='limited'"
        )


def get_curious_records_by_type(df: pd.DataFrame, account_type: str) -> pd.DataFrame:
    """Get records filtered by account type."""
    return df[df["accountType"] == account_type]


def get_curious_records_by_tag(df: pd.DataFrame, tag: str) -> pd.DataFrame:
    """Get records filtered by tag (parent/child)."""
    return df[df["tag"] == tag]


def count_curious_accounts(df: pd.DataFrame) -> dict[str, int]:
    """Count accounts by tag (parent/child) not accountType."""
    return {
        "parent": len(df[df["tag"] == "parent"]),
        "child": len(df[df["tag"] == "child"]),
        "total": len(df),
    }


def assert_no_none_in_records(records: list[dict]) -> None:
    """Assert that no None values exist in record dictionaries."""
    for record in records:
        for key, value in record.items():
            assert value is not None, f"Field '{key}' should not be None in record"


def assert_enrollment_complete_updated(df: pd.DataFrame, expected_value: str) -> None:
    """Assert enrollment_complete field has expected value."""
    enrollment_rows = df[df["field_name"] == "enrollment_complete"]
    assert len(enrollment_rows) > 0, "No enrollment_complete field found"
    assert all(enrollment_rows["value"] == expected_value), (
        f"All enrollment_complete values should be '{expected_value}'"
    )


# ============================================================================
# Helper Functions - Mock Setup
# ============================================================================


def setup_curious_integration_mocks(
    curious_vars_mock: Mock,
    redcap_vars_mock: Mock,
) -> None:
    """Set up common mock configurations for integration tests."""
    # REDCap configuration
    redcap_vars_mock.Tokens.pid247 = "token_247"
    redcap_vars_mock.headers = {}
    redcap_vars_mock.Endpoints.return_value.base_url = "https://redcap.test/api/"

    # Curious configuration
    curious_vars_mock.applet_ids = {
        "Healthy Brain Network Questionnaires": "test_applet"
    }
    curious_tokens = Mock()
    curious_tokens.access = "test_token"
    curious_tokens.endpoints.base_url = "https://curious.test/"
    curious_vars_mock.Tokens.return_value = curious_tokens
    curious_vars_mock.headers = {}


def create_curious_api_failure(message: str = "API Error"):
    """Create a RequestException for testing."""
    return requests.exceptions.RequestException(message)


# ============================================================================
# Module Mocking
# ============================================================================


@contextmanager
def create_mock_module_in_sys(
    module_path: str, attributes: Optional[Dict[str, Any]] = None
):
    """
    Return context manager to temporarily add mock module to sys.modules.

    Parameters
    ----------
    module_path
        String path like 'parent.child.module'
    attributes
        Dict of attributes to add to module

    """
    mock_mod = Mock()
    if attributes:
        for key, value in attributes.items():
            setattr(mock_mod, key, value)
    with patch.dict("sys.modules", {module_path: mock_mod}):
        yield mock_mod
