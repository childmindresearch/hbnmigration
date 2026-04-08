"""Tests for from_curious.utils module."""

from unittest.mock import Mock, patch

import pandas as pd
import pytest

from hbnmigration.from_curious.utils import (
    alert_websocket_to_https,
    ALERTS_INSTRUMENT_FORM,
    call_curious_api,
    create_choice_lookup,
    DEFAULT_EVENT_FOR_ALERTS,
    fetch_alerts_metadata,
    get_alert_field_event,
    get_alert_form_for_instrument,
    get_field_to_event_mapping,
    get_instrument_event_mapping,
    METADATA_PARAMS,
    possible_alert_instruments,
    REDCAP_TOKEN,
)
from hbnmigration.utility_functions import CuriousAlertHttps, CuriousAlertWebsocket

# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def mock_fetch_api_data():
    """Mock fetch_api_data function."""
    with patch("hbnmigration.from_curious.utils.fetch_api_data") as mock:
        yield mock


@pytest.fixture
def sample_alerts_metadata():
    """Sample alerts metadata from REDCap."""
    return pd.DataFrame(
        {
            "field_name": [
                "mrn",
                "alerts_parent_baseline_1",
                "alerts_parent_baseline_2",
                "alerts_parent_followup_1",
                "alerts_child_baseline_1",
                "parent_baseline_alerts",
                "parent_followup_alerts",
                "child_baseline_alerts",
            ],
            "form_name": [
                "ra_alerts_parent",
                "ra_alerts_parent",
                "ra_alerts_parent",
                "ra_alerts_parent",
                "ra_alerts_child",
                "ra_alerts_parent",
                "ra_alerts_parent",
                "ra_alerts_child",
            ],
            "select_choices_or_calculations": [
                "",
                "0, No | 1, Yes",
                "0, No | 1, Yes | 2, Maybe",
                "0, No | 1, Yes",
                "0, No | 1, Yes",
                "0, No | 1, Yes",
                "0, No | 1, Yes",
                "0, No | 1, Yes",
            ],
        }
    )


@pytest.fixture
def sample_instrument_event_mapping():
    """Sample instrument-event mapping from REDCap."""
    return pd.DataFrame(
        {
            "form": [
                "ace_p",
                "ace_c",
                "ra_alerts_parent",
                "ra_alerts_child",
                "baseline_data",
            ],
            "unique_event_name": [
                "baseline_arm_1",
                "baseline_arm_1",
                "admin_arm_1",
                "admin_arm_1",
                "baseline_arm_1",
            ],
        }
    )


@pytest.fixture
def sample_field_to_event_data():
    """Sample field-to-event data from REDCap."""
    return pd.DataFrame(
        {
            "field_name": [
                "alerts_parent_baseline_1",
                "alerts_parent_baseline_2",
                "parent_baseline_alerts",
            ],
            "redcap_event_name": [
                "admin_arm_1",
                "admin_arm_1",
                "admin_arm_1",
            ],
        }
    )


@pytest.fixture
def mock_tokens():
    """Mock Curious tokens."""
    mock = Mock()
    mock.access = "test_token"
    mock.endpoints = Mock()
    mock.endpoints.alerts = "https://curious.test/alerts"
    return mock


@pytest.fixture
def websocket_alert() -> CuriousAlertWebsocket:
    """Create websocket format alert."""
    return {
        "id": "alert_001",
        "isWatched": False,
        "appletId": "applet_123",
        "appletName": "Test Applet",
        "version": "1.0.0",
        "secret_id": "00001_P",
        "activityId": "activity_123",
        "activityItemId": "item_123",
        "message": "Test message",
        "createdAt": "2024-01-01T00:00:00Z",
        "answerId": "answer_123",
        "encryption": {
            "base": "base",
            "prime": "prime",
            "accountId": "account_123",
            "publicKey": "key",
        },
        "workspace": "workspace_1",
        "respondentId": "respondent_123",
        "subjectId": "subject_123",
        "type": "answer",
    }


@pytest.fixture
def https_alert() -> CuriousAlertHttps:
    """Create HTTPS format alert."""
    return {
        "id": "alert_001",
        "isWatched": False,
        "appletId": "applet_123",
        "appletName": "Test Applet",
        "version": "1.0.0",
        "secretId": "00001_P",  # HTTPS format
        "activityId": "activity_123",
        "activityItemId": "item_123",
        "message": "Test message",
        "createdAt": "2024-01-01T00:00:00Z",
        "answerId": "answer_123",
        "encryption": {
            "base": "base",
            "prime": "prime",
            "accountId": "account_123",
            "publicKey": "key",
        },
        "workspace": "workspace_1",
        "respondentId": "respondent_123",
        "subjectId": "subject_123",
        "type": "answer",
    }


# ============================================================================
# Tests - Constants
# ============================================================================


def test_constants_are_defined():
    """Test that required constants are defined."""
    assert REDCAP_TOKEN is not None
    assert ALERTS_INSTRUMENT_FORM == "ra_alerts_child,ra_alerts_parent"
    assert DEFAULT_EVENT_FOR_ALERTS == "admin_arm_1"
    assert isinstance(METADATA_PARAMS, dict)


def test_metadata_params_has_required_keys():
    """Test that METADATA_PARAMS has all required keys."""
    required_keys = {
        "content",
        "action",
        "format",
        "type",
        "csvDelimiter",
        "rawOrLabel",
        "rawOrLabelHeaders",
        "exportCheckboxLabel",
        "exportSurveyFields",
        "exportDataAccessGroups",
        "returnFormat",
    }
    assert required_keys.issubset(METADATA_PARAMS.keys())


# ============================================================================
# Tests - call_curious_api
# ============================================================================


def test_call_curious_api_success(mock_tokens):
    """Test successful API call."""
    with patch("hbnmigration.from_curious.utils.requests.get") as mock_get:
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"result": [{"id": "123"}]}
        mock_get.return_value = mock_response

        result = call_curious_api(
            "https://curious.test/endpoint",
            mock_tokens,
        )

        assert result == [{"id": "123"}]
        assert mock_get.called


def test_call_curious_api_with_return_type(mock_tokens):
    """Test API call with return type."""
    with patch("hbnmigration.from_curious.utils.requests.get") as mock_get:
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"result": [{"id": "123"}]}
        mock_get.return_value = mock_response

        result = call_curious_api(
            "https://curious.test/endpoint",
            mock_tokens,
            return_type=list,
        )

        assert result == [{"id": "123"}]


def test_call_curious_api_error(mock_tokens):
    """Test API call with error."""
    with patch("hbnmigration.from_curious.utils.requests.get") as mock_get:
        mock_response = Mock()
        mock_response.status_code = 500
        mock_get.return_value = mock_response

        with pytest.raises(Exception):
            call_curious_api(
                "https://curious.test/endpoint",
                mock_tokens,
            )


def test_call_curious_api_custom_headers(mock_tokens):
    """Test API call with custom headers."""
    with patch("hbnmigration.from_curious.utils.requests.get") as mock_get:
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"result": {}}
        mock_get.return_value = mock_response

        custom_headers = {"X-Custom": "header"}
        call_curious_api(
            "https://curious.test/endpoint",
            mock_tokens,
            headers=custom_headers,
        )

        assert mock_get.call_args[1]["headers"] == custom_headers


# ============================================================================
# Tests - alert_websocket_to_https
# ============================================================================


def test_alert_websocket_to_https_conversion(websocket_alert):
    """Test conversion from websocket to https format."""
    result = alert_websocket_to_https(websocket_alert)
    assert "secretId" in result
    assert result["secretId"] == websocket_alert["secret_id"]


def test_alert_websocket_to_https_already_https(https_alert):
    """Test that https format alert passes through unchanged."""
    result = alert_websocket_to_https(https_alert)
    assert result == https_alert
    assert "secretId" in result


def test_alert_websocket_to_https_preserves_other_fields(websocket_alert):
    """Test that all other fields are preserved."""
    result = alert_websocket_to_https(websocket_alert)
    for key in websocket_alert:
        if key != "secret_id":
            assert key in result
            assert result[key] == websocket_alert[key]


# ============================================================================
# Tests - create_choice_lookup
# ============================================================================


def test_create_choice_lookup_creates_dict(sample_alerts_metadata):
    """Test that create_choice_lookup creates correct mapping."""
    result = create_choice_lookup(sample_alerts_metadata)
    assert isinstance(result, dict)
    # Check a known mapping
    assert ("alerts_parent_baseline_1", "no") in result
    assert result[("alerts_parent_baseline_1", "no")] == "0"
    assert result[("alerts_parent_baseline_1", "yes")] == "1"


def test_create_choice_lookup_handles_multiple_choices(sample_alerts_metadata):
    """Test lookup with field having multiple choices."""
    result = create_choice_lookup(sample_alerts_metadata)
    # alerts_parent_baseline_2 has 3 choices
    assert ("alerts_parent_baseline_2", "no") in result
    assert ("alerts_parent_baseline_2", "yes") in result
    assert ("alerts_parent_baseline_2", "maybe") in result
    assert result[("alerts_parent_baseline_2", "maybe")] == "2"


def test_create_choice_lookup_skips_text_fields(sample_alerts_metadata):
    """Test that text fields (no choices) are skipped."""
    result = create_choice_lookup(sample_alerts_metadata)
    # mrn has no choices
    assert not any(key[0] == "mrn" for key in result.keys())


def test_create_choice_lookup_empty_metadata():
    """Test with empty metadata."""
    empty_df = pd.DataFrame(columns=["field_name", "select_choices_or_calculations"])
    result = create_choice_lookup(empty_df)
    assert result == {}


def test_create_choice_lookup_returns_strings():
    """Test that lookup returns string indices."""
    metadata = pd.DataFrame(
        {
            "field_name": ["test_field"],
            "select_choices_or_calculations": ["0, No | 1, Yes"],
        }
    )
    result = create_choice_lookup(metadata)
    assert isinstance(result[("test_field", "no")], str)
    assert result[("test_field", "no")] == "0"


# ============================================================================
# Tests - fetch_alerts_metadata
# ============================================================================


def test_fetch_alerts_metadata_calls_api(mock_fetch_api_data, sample_alerts_metadata):
    """Test that fetch_alerts_metadata calls fetch_api_data with correct params."""
    mock_fetch_api_data.return_value = sample_alerts_metadata
    base_url = "https://redcap.example.com/api/"
    result = fetch_alerts_metadata(base_url)
    assert mock_fetch_api_data.called
    call_args = mock_fetch_api_data.call_args
    assert call_args[0][0] == base_url
    assert "forms" in call_args[0][2]
    assert call_args[0][2]["forms"] == ALERTS_INSTRUMENT_FORM
    assert call_args[0][2]["token"] == REDCAP_TOKEN
    pd.testing.assert_frame_equal(result, sample_alerts_metadata)


def test_fetch_alerts_metadata_passes_metadata_params(mock_fetch_api_data):
    """Test that fetch_alerts_metadata includes all METADATA_PARAMS."""
    mock_fetch_api_data.return_value = pd.DataFrame()
    base_url = "https://redcap.example.com/api/"
    fetch_alerts_metadata(base_url)
    call_params = mock_fetch_api_data.call_args[0][2]
    for key, value in METADATA_PARAMS.items():
        assert key in call_params
        assert call_params[key] == value


# ============================================================================
# Tests - possible_alert_instruments
# ============================================================================


def test_possible_alert_instruments_extracts_field_names(
    mock_fetch_api_data, sample_alerts_metadata
):
    """Test that possible_alert_instruments extracts alert instruments correctly."""
    mock_fetch_api_data.return_value = sample_alerts_metadata
    base_url = "https://redcap.example.com/api/"
    result = possible_alert_instruments(base_url)
    assert mock_fetch_api_data.called
    # Function returns instrument names (without _alerts suffix)
    expected = {
        "parent_baseline",
        "parent_followup",
        "child_baseline",
    }
    assert set(result) == expected


def test_possible_alert_instruments_empty_metadata(mock_fetch_api_data):
    """Test possible_alert_instruments with empty metadata."""
    mock_fetch_api_data.return_value = pd.DataFrame(columns=["field_name"])
    base_url = "https://redcap.example.com/api/"
    result = possible_alert_instruments(base_url)
    assert result == []


@pytest.mark.parametrize(
    "field_names,expected_instruments",
    [
        (
            # Only summary fields (ending in _alerts) are processed
            ["parent_baseline_alerts", "child_followup_alerts"],
            ["parent_baseline", "child_followup"],
        ),
        (["mrn", "date_of_birth"], []),
        (
            # Duplicates are removed by unique()
            ["parent_baseline_alerts", "parent_baseline_alerts", "mrn"],
            ["parent_baseline"],
        ),
    ],
    ids=["summary_fields", "no_alerts", "duplicates"],
)
def test_possible_alert_instruments_various_fields(
    mock_fetch_api_data, field_names, expected_instruments
):
    """Test possible_alert_instruments with various field lists."""
    metadata = pd.DataFrame({"field_name": field_names})
    mock_fetch_api_data.return_value = metadata
    base_url = "https://redcap.example.com/api/"
    result = possible_alert_instruments(base_url)
    assert sorted(result) == sorted(expected_instruments)


# ============================================================================
# Tests - get_instrument_event_mapping
# ============================================================================


def test_get_instrument_event_mapping_returns_dict(
    mock_fetch_api_data, sample_instrument_event_mapping
):
    """Test that get_instrument_event_mapping returns instrument-to-event dict."""
    mock_fetch_api_data.return_value = sample_instrument_event_mapping
    base_url = "https://redcap.example.com/api/"
    result = get_instrument_event_mapping(base_url)
    assert isinstance(result, dict)
    assert result["ra_alerts_parent"] == "admin_arm_1"
    assert result["ace_p"] == "baseline_arm_1"


def test_get_instrument_event_mapping_skips_duplicates(mock_fetch_api_data):
    """Test that only the first event is used for each instrument."""
    data = pd.DataFrame(
        {
            "form": ["ace_p", "ace_p", "ra_alerts_parent"],
            "unique_event_name": ["baseline_arm_1", "followup_arm_1", "admin_arm_1"],
        }
    )
    mock_fetch_api_data.return_value = data
    base_url = "https://redcap.example.com/api/"
    result = get_instrument_event_mapping(base_url)
    # Should use first event for ace_p
    assert result["ace_p"] == "baseline_arm_1"
    assert result["ra_alerts_parent"] == "admin_arm_1"


def test_get_instrument_event_mapping_empty_response(mock_fetch_api_data, caplog):
    """Test get_instrument_event_mapping with empty response."""
    mock_fetch_api_data.return_value = pd.DataFrame()
    result = get_instrument_event_mapping("https://redcap.example.com/api/")
    assert result == {}
    assert "No instrument-event mapping found" in caplog.text


def test_get_instrument_event_mapping_handles_exception(mock_fetch_api_data, caplog):
    """Test that get_instrument_event_mapping handles exceptions gracefully."""
    mock_fetch_api_data.side_effect = Exception("API Error")
    result = get_instrument_event_mapping("https://redcap.example.com/api/")
    assert result == {}
    assert "Could not fetch instrument-event mapping" in caplog.text


# ============================================================================
# Tests - get_alert_form_for_instrument
# ============================================================================


@pytest.mark.parametrize(
    "instrument_name,expected_form",
    [
        ("ace_p", "ra_alerts_parent"),
        ("cbcl_p", "ra_alerts_parent"),
        ("ace_c", "ra_alerts_child"),
        ("cbcl_c", "ra_alerts_child"),
        ("parent_baseline", "ra_alerts_parent"),
        ("unknown_instrument", "ra_alerts_child"),  # default
    ],
    ids=[
        "parent_suffix",
        "parent_suffix_2",
        "child_suffix",
        "child_suffix_2",
        "parent_pattern",
        "default",
    ],
)
def test_get_alert_form_for_instrument(instrument_name, expected_form):
    """Test get_alert_form_for_instrument with various instrument names."""
    result = get_alert_form_for_instrument(instrument_name)
    assert result == expected_form


# ============================================================================
# Tests - get_alert_field_event
# ============================================================================


def test_get_alert_field_event_returns_event_from_mapping(
    mock_fetch_api_data, sample_instrument_event_mapping
):
    """Test that get_alert_field_event returns correct event."""
    mock_fetch_api_data.return_value = sample_instrument_event_mapping
    base_url = "https://redcap.example.com/api/"
    result = get_alert_field_event(base_url, "ace_p")
    assert result == "admin_arm_1"
    assert mock_fetch_api_data.called


def test_get_alert_field_event_uses_default_when_not_found(mock_fetch_api_data, caplog):
    """Test that get_alert_field_event uses default when event not found."""
    # Return empty mapping
    mock_fetch_api_data.return_value = pd.DataFrame(
        {"form": [], "unique_event_name": []}
    )
    base_url = "https://redcap.example.com/api/"
    result = get_alert_field_event(base_url, "unknown_instrument")
    assert result == DEFAULT_EVENT_FOR_ALERTS
    assert "using default" in caplog.text


@pytest.mark.parametrize(
    "instrument_name",
    ["ace_p", "parent_baseline", "random_p"],
    ids=["parent_p_suffix", "parent_pattern", "random_parent"],
)
def test_get_alert_field_event_for_parent_instruments(
    mock_fetch_api_data, sample_instrument_event_mapping, instrument_name
):
    """Test get_alert_field_event for various parent instruments."""
    mock_fetch_api_data.return_value = sample_instrument_event_mapping
    base_url = "https://redcap.example.com/api/"
    result = get_alert_field_event(base_url, instrument_name)
    # All parent instruments should use ra_alerts_parent form's event
    assert result == "admin_arm_1"


# ============================================================================
# Tests - get_field_to_event_mapping
# ============================================================================


def test_get_field_to_event_mapping_returns_field_to_event_dict(
    mock_fetch_api_data, sample_field_to_event_data
):
    """Test that get_field_to_event_mapping returns field-to-event dict."""
    mock_fetch_api_data.return_value = sample_field_to_event_data
    base_url = "https://redcap.example.com/api/"
    field_names = [
        "alerts_parent_baseline_1",
        "alerts_parent_baseline_2",
        "parent_baseline_alerts",
    ]
    result = get_field_to_event_mapping(base_url, field_names)
    assert isinstance(result, dict)
    assert all(v == "admin_arm_1" for v in result.values())


def test_get_field_to_event_mapping_empty_field_names(mock_fetch_api_data):
    """Test get_field_to_event_mapping with empty field list."""
    result = get_field_to_event_mapping("https://redcap.example.com/api/", [])
    assert result == {}
    assert not mock_fetch_api_data.called


def test_get_field_to_event_mapping_empty_response(mock_fetch_api_data, caplog):
    """Test get_field_to_event_mapping with empty response."""
    mock_fetch_api_data.return_value = pd.DataFrame()
    base_url = "https://redcap.example.com/api/"
    field_names = ["alerts_parent_baseline_1"]
    result = get_field_to_event_mapping(base_url, field_names)
    assert result == {}
    assert "No existing REDCap data found" in caplog.text


def test_get_field_to_event_mapping_handles_exception(mock_fetch_api_data, caplog):
    """Test that get_field_to_event_mapping handles exceptions gracefully."""
    mock_fetch_api_data.side_effect = Exception("API Error")
    base_url = "https://redcap.example.com/api/"
    field_names = ["alerts_parent_baseline_1"]
    result = get_field_to_event_mapping(base_url, field_names)
    assert result == {}
    assert "Could not fetch field-to-event mapping" in caplog.text


@pytest.mark.parametrize(
    "field_names,data,expected_count",
    [
        (
            ["alerts_parent_baseline_1", "alerts_child_followup_1"],
            pd.DataFrame(
                {
                    "field_name": [
                        "alerts_parent_baseline_1",
                        "alerts_child_followup_1",
                    ],
                    "redcap_event_name": ["admin_arm_1", "admin_arm_1"],
                }
            ),
            2,
        ),
        (
            ["single_field"],
            pd.DataFrame(
                {
                    "field_name": ["single_field"],
                    "redcap_event_name": ["baseline_arm_1"],
                }
            ),
            1,
        ),
    ],
    ids=["multiple_fields", "single_field"],
)
def test_get_field_to_event_mapping_various_inputs(
    mock_fetch_api_data, field_names, data, expected_count
):
    """Test get_field_to_event_mapping with various field configurations."""
    mock_fetch_api_data.return_value = data
    base_url = "https://redcap.example.com/api/"
    result = get_field_to_event_mapping(base_url, field_names)
    assert len(result) == expected_count


# ============================================================================
# Tests - Integration with data_to_redcap
# ============================================================================


def test_utils_compatible_with_data_to_redcap_workflow(
    mock_fetch_api_data, sample_alerts_metadata, sample_instrument_event_mapping
):
    """Test that utils functions work together for data_to_redcap workflow."""
    base_url = "https://redcap.example.com/api/"
    # Test the full workflow
    mock_fetch_api_data.side_effect = [
        sample_alerts_metadata,
        sample_instrument_event_mapping,
    ]
    # Step 1: Get possible instruments
    instruments = possible_alert_instruments(base_url)
    assert "parent_baseline" in instruments
    # Step 2: For each instrument, get the alert event
    for instrument in instruments:
        event = get_alert_field_event(base_url, instrument)
        assert event in ["admin_arm_1", "baseline_arm_1"]
