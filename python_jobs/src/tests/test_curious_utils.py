"""Tests for from_curious.utils module."""

from unittest.mock import patch

import pandas as pd
import pytest

from hbnmigration.from_curious.utils import (
    ALERTS_INSTRUMENT_FORM,
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
