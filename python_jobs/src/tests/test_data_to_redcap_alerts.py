"""Tests for add_alert_fields_if_needed function."""

from unittest.mock import MagicMock, patch

import pandas as pd
import polars as pl
import pytest

from hbnmigration.from_curious.data_to_redcap import (
    _build_alert_values,
    _determine_event_column,
    _determine_event_for_alert,
    _determine_record_id_column,
    add_alert_fields_if_needed,
    get_redcap_records_for_instrument,
)
from mindlogger_data_export.outputs import NamedOutput

# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def sample_csv_path(tmp_path):
    """Create a temporary CSV file for testing."""
    return tmp_path / "test_instrument.csv"


@pytest.fixture
def sample_dataframe():
    """Create a sample DataFrame for testing."""
    return pl.DataFrame(
        {
            "record_id": ["001", "002", "003"],
            "field1": ["value1", "value2", "value3"],
            "field2": [1, 2, 3],
        }
    )


@pytest.fixture
def sample_dataframe_with_event():
    """Create a sample DataFrame with event column."""
    return pl.DataFrame(
        {
            "record_id": ["001", "002", "003"],
            "redcap_event_name": ["baseline_arm_1", "baseline_arm_1", "followup_arm_1"],
            "field1": ["value1", "value2", "value3"],
        }
    )


@pytest.fixture
def sample_metadata_empty():
    """Sample empty metadata."""
    return pd.DataFrame()


@pytest.fixture
def sample_metadata_with_alert_field():
    """Sample metadata including alert field."""
    return pd.DataFrame(
        {
            "field_name": [
                "record_id",
                "field1",
                "instrument_alerts",
                "field2",
            ]
        }
    )


@pytest.fixture
def sample_existing_redcap_data_no_alerts():
    """Sample REDCap data with no alerts set."""
    return {
        "001": {"record_id": "001", "instrument_alerts": ""},
        "002": {"record_id": "002", "instrument_alerts": ""},
        "003": {"record_id": "003", "instrument_alerts": ""},
    }


@pytest.fixture
def sample_existing_redcap_data_with_alerts():
    """Sample REDCap data with some alerts already set."""
    return {
        "001": {"record_id": "001", "instrument_alerts": "yes"},
        "002": {"record_id": "002", "instrument_alerts": ""},
        "003": {"record_id": "003", "instrument_alerts": "no"},
    }


@pytest.fixture
def named_output_with_mixed_subjects():
    """Create a NamedOutput with both child and parent (suffix _P) target subjects."""
    df = pl.DataFrame(
        {
            "record_id": ["001", "002", "003", "004"],
            "target_user_secret_id": ["12345", "12346", "12347_P", "12348_P"],
            "activity_name": ["baseline", "baseline", "baseline", "baseline"],
            "item_response": ["yes", "no", "yes", "no"],
        }
    )
    return NamedOutput(name="test_activity_redcap", output=df)


@pytest.fixture
def named_output_no_target_column():
    """Create a NamedOutput without target_user_secret_id column."""
    df = pl.DataFrame(
        {
            "record_id": ["001", "002"],
            "activity_name": ["baseline", "baseline"],
            "item_response": ["yes", "no"],
        }
    )
    return NamedOutput(name="test_activity_redcap", output=df)


@pytest.fixture
def named_output_all_child_subjects():
    """Create a NamedOutput with only child (non-_P) target subjects."""
    df = pl.DataFrame(
        {
            "record_id": ["001", "002", "003"],
            "target_user_secret_id": ["12345", "12346", "12347"],
            "activity_name": ["baseline", "baseline", "baseline"],
            "item_response": ["yes", "no", "yes"],
        }
    )
    return NamedOutput(name="test_activity_redcap", output=df)


@pytest.fixture
def named_output_all_parent_subjects():
    """Create a NamedOutput with only parent (_P) target subjects."""
    df = pl.DataFrame(
        {
            "record_id": ["001", "002"],
            "target_user_secret_id": ["12345_P", "12346_P"],
            "activity_name": ["baseline", "baseline"],
            "item_response": ["yes", "no"],
        }
    )
    return NamedOutput(name="test_activity_redcap", output=df)


# ============================================================================
# Tests - _determine_record_id_column
# ============================================================================


@pytest.mark.parametrize(
    "columns,expected",
    [
        (["record_id", "field1", "field2"], "record_id"),
        (["record", "field1", "field2"], "record"),
        (["participant_id", "field1", "field2"], "participant_id"),
        (["subject_id", "field1", "field2"], "subject_id"),
        (["field1", "field2", "field3"], None),
    ],
    ids=["record_id", "record", "participant_id", "subject_id", "no_match"],
)
def test_determine_record_id_column(columns, expected):
    """Test _determine_record_id_column with various column names."""
    df = pl.DataFrame({col: ["val"] for col in columns})
    result = _determine_record_id_column(df)
    assert result == expected


# ============================================================================
# Tests - _determine_event_column
# ============================================================================


@pytest.mark.parametrize(
    "columns,expected",
    [
        (["redcap_event_name", "field1", "field2"], "redcap_event_name"),
        (["event", "field1", "field2"], "event"),
        (["field1", "field2", "field3"], None),
    ],
    ids=["redcap_event_name", "event", "no_match"],
)
def test_determine_event_column(columns, expected):
    """Test _determine_event_column with various column names."""
    df = pl.DataFrame({col: ["val"] for col in columns})
    result = _determine_event_column(df)
    assert result == expected


# ============================================================================
# Tests - _determine_event_for_alert
# ============================================================================


def test_determine_event_for_alert_from_dataframe():
    """Test _determine_event_for_alert uses event from dataframe."""
    df = pl.DataFrame(
        {
            "redcap_event_name": ["baseline_arm_1", "followup_arm_1"],
            "field1": [1, 2],
        }
    )
    event = _determine_event_for_alert(df, "redcap_event_name", "test_instrument", "")
    assert event == "baseline_arm_1"


@patch("hbnmigration.from_curious.data_to_redcap.get_alert_field_event")
def test_determine_event_for_alert_from_api(mock_get_event):
    """Test _determine_event_for_alert queries API when no event column."""
    mock_get_event.return_value = "admin_arm_1"
    df = pl.DataFrame({"field1": [1, 2]})
    event = _determine_event_for_alert(
        df, None, "test_instrument", "https://redcap.example.com/api/"
    )
    assert event == "admin_arm_1"
    assert mock_get_event.called


def test_determine_event_for_alert_empty_dataframe():
    """Test _determine_event_for_alert with empty dataframe."""
    df = pl.DataFrame({"redcap_event_name": pl.Series([], dtype=pl.Utf8)})
    with patch(
        "hbnmigration.from_curious.data_to_redcap.get_alert_field_event"
    ) as mock_get_event:
        mock_get_event.return_value = "admin_arm_1"
        event = _determine_event_for_alert(
            df,
            "redcap_event_name",
            "test_instrument",
            "https://redcap.example.com/api/",
        )
        assert event == "admin_arm_1"


# ============================================================================
# Tests - _build_alert_values
# ============================================================================


def test_build_alert_values_no_existing_alerts(sample_dataframe):
    """Test _build_alert_values when no alerts exist in REDCap."""
    existing_data = {
        "001": {"instrument_alerts": ""},
        "002": {"instrument_alerts": ""},
        "003": {"instrument_alerts": ""},
    }
    alert_values, _event_values, with_alerts, setting_no = _build_alert_values(
        sample_dataframe,
        "record_id",
        None,
        "baseline_arm_1",
        "instrument_alerts",
        existing_data,
    )
    assert alert_values == ["no", "no", "no"]
    assert setting_no == 3
    assert with_alerts == 0


def test_build_alert_values_preserves_existing_yes():
    """Test _build_alert_values preserves existing 'yes' alerts."""
    df = pl.DataFrame({"record_id": ["001", "002", "003"]})
    existing_data = {
        "001": {"instrument_alerts": "yes"},
        "002": {"instrument_alerts": ""},
        "003": {"instrument_alerts": "yes"},
    }
    alert_values, _, with_alerts, setting_no = _build_alert_values(
        df, "record_id", None, "baseline_arm_1", "instrument_alerts", existing_data
    )
    assert alert_values == ["", "no", ""]
    assert with_alerts == 2
    assert setting_no == 1


def test_build_alert_values_with_event_column(sample_dataframe_with_event):
    """Test _build_alert_values with event column in dataframe."""
    existing_data = {
        "001": {"instrument_alerts": ""},
        "002": {"instrument_alerts": ""},
        "003": {"instrument_alerts": ""},
    }
    alert_values, event_values, _, _ = _build_alert_values(
        sample_dataframe_with_event,
        "record_id",
        "redcap_event_name",
        None,
        "instrument_alerts",
        existing_data,
    )
    assert alert_values == ["no", "no", "no"]
    assert event_values == ["baseline_arm_1", "baseline_arm_1", "followup_arm_1"]


@pytest.mark.parametrize(
    "existing_status,expected_alert,expected_count",
    [
        ("yes", "", 1),  # Preserve yes
        ("", "no", 0),  # Set no
        ("no", "no", 0),  # Overwrite no with no
        ("maybe", "no", 0),  # Overwrite invalid with no
    ],
    ids=["preserve_yes", "empty_to_no", "no_to_no", "invalid_to_no"],
)
def test_build_alert_values_various_statuses(
    existing_status, expected_alert, expected_count
):
    """Test _build_alert_values with various existing statuses."""
    df = pl.DataFrame({"record_id": ["001"]})
    existing_data = {"001": {"instrument_alerts": existing_status}}
    alert_values, _, with_alerts, _ = _build_alert_values(
        df, "record_id", None, "baseline_arm_1", "instrument_alerts", existing_data
    )
    assert alert_values[0] == expected_alert
    assert with_alerts == expected_count


# ============================================================================
# Tests - get_redcap_records_for_instrument
# ============================================================================


@patch("hbnmigration.from_curious.data_to_redcap.fetch_api_data")
@patch("hbnmigration.from_curious.data_to_redcap.requests.post")
def test_get_redcap_records_for_instrument_fetches_metadata(
    mock_post, mock_fetch_api, caplog
):
    """Test get_redcap_records_for_instrument fetches metadata."""
    mock_fetch_api.return_value = pd.DataFrame(
        {"field_name": ["record_id", "instrument_alerts", "field1"]}
    )
    mock_post.return_value = MagicMock(json=lambda: [])
    mock_post.return_value.raise_for_status = MagicMock()

    result = get_redcap_records_for_instrument("test_instrument", ["001", "002"])

    assert mock_fetch_api.called
    assert result == {}


@patch("hbnmigration.from_curious.data_to_redcap.fetch_api_data")
def test_get_redcap_records_for_instrument_empty_metadata(mock_fetch_api, caplog):
    """Test get_redcap_records_for_instrument with empty metadata."""
    mock_fetch_api.return_value = pd.DataFrame()

    result = get_redcap_records_for_instrument("test_instrument", ["001", "002"])

    assert result == {}
    assert "No metadata found" in caplog.text


@patch("hbnmigration.from_curious.data_to_redcap.fetch_api_data")
def test_get_redcap_records_for_instrument_no_alert_field(mock_fetch_api):
    """Test get_redcap_records_for_instrument when alert field doesn't exist."""
    mock_fetch_api.return_value = pd.DataFrame(
        {"field_name": ["record_id", "field1", "field2"]}
    )

    result = get_redcap_records_for_instrument("test_instrument", ["001", "002"])

    assert result == {}
    # Verify that function was called and returned empty
    assert mock_fetch_api.called


@patch("hbnmigration.from_curious.data_to_redcap.fetch_api_data")
@patch("hbnmigration.from_curious.data_to_redcap.requests.post")
def test_get_redcap_records_for_instrument_returns_dict_by_record_id(
    mock_post, mock_fetch_api
):
    """Test get_redcap_records_for_instrument returns data keyed by record_id."""
    mock_fetch_api.return_value = pd.DataFrame(
        {"field_name": ["record_id", "test_instrument_alerts"]}
    )
    mock_response = MagicMock()
    mock_response.json.return_value = [
        {"record_id": "001", "test_instrument_alerts": "yes"},
        {"record_id": "002", "test_instrument_alerts": ""},
    ]
    mock_response.raise_for_status = MagicMock()
    mock_post.return_value = mock_response

    result = get_redcap_records_for_instrument("test_instrument", ["001", "002"])

    assert "001" in result
    assert "002" in result
    assert result["001"]["test_instrument_alerts"] == "yes"
    assert result["002"]["test_instrument_alerts"] == ""


@patch("hbnmigration.from_curious.data_to_redcap.fetch_api_data")
@patch("hbnmigration.from_curious.data_to_redcap.requests.post")
def test_get_redcap_records_for_instrument_handles_exception(
    mock_post, mock_fetch_api, caplog
):
    """Test get_redcap_records_for_instrument handles API errors."""
    mock_fetch_api.return_value = pd.DataFrame(
        {"field_name": ["record_id", "test_instrument_alerts"]}
    )
    mock_post.side_effect = Exception("API Error")

    result = get_redcap_records_for_instrument("test_instrument", ["001"])

    assert result == {}
    assert "Could not fetch REDCap data" in caplog.text


# ============================================================================
# Tests - add_alert_fields_if_needed
# ============================================================================


@patch("hbnmigration.from_curious.data_to_redcap.time.sleep")
@patch("hbnmigration.from_curious.data_to_redcap.get_alert_field_event")
@patch("hbnmigration.from_curious.data_to_redcap.possible_alert_instruments")
def test_add_alert_fields_skips_non_alert_instruments_case(
    mock_possible_alerts, mock_get_event, mock_sleep, sample_csv_path
):
    """Test that non-alert instruments are skipped."""
    df = pl.DataFrame({"record_id": ["001"], "field1": [1]})
    df.write_csv(sample_csv_path)
    # Return type should be list of instrument names
    mock_possible_alerts.return_value = []  # Empty list = no alert instruments

    add_alert_fields_if_needed(sample_csv_path)

    # If instruments is empty, function should return early
    assert not mock_get_event.called
    # CSV shouldn't be modified
    result = pl.read_csv(sample_csv_path)
    assert "test_instrument_alerts" not in result.columns


@patch("hbnmigration.from_curious.data_to_redcap.time.sleep")
@patch("hbnmigration.from_curious.data_to_redcap.get_alert_field_event")
@patch("hbnmigration.from_curious.data_to_redcap.get_redcap_records_for_instrument")
@patch("hbnmigration.from_curious.data_to_redcap.possible_alert_instruments")
def test_add_alert_fields_skips_when_alert_field_exists(
    mock_possible_alerts, mock_get_records, mock_get_event, mock_sleep, sample_csv_path
):
    """Test that function returns early if alert field exists in data."""
    df = pl.DataFrame(
        {"record_id": ["001"], "test_instrument_alerts": ["yes"], "field1": [1]}
    )
    df.write_csv(sample_csv_path)
    mock_possible_alerts.return_value = ["test_instrument"]

    add_alert_fields_if_needed(sample_csv_path)

    assert not mock_get_records.called
    assert not mock_get_event.called


@patch("hbnmigration.from_curious.data_to_redcap.time.sleep")
@patch("hbnmigration.from_curious.data_to_redcap.get_alert_field_event")
@patch("hbnmigration.from_curious.data_to_redcap.get_redcap_records_for_instrument")
@patch("hbnmigration.from_curious.data_to_redcap.possible_alert_instruments")
def test_add_alert_fields_skips_without_record_id(
    mock_possible_alerts,
    mock_get_records,
    mock_get_event,
    mock_sleep,
    sample_csv_path,
    caplog,
):
    """Test that function returns if record ID column not found."""
    df = pl.DataFrame({"field1": ["001"], "field2": [1]})
    df.write_csv(sample_csv_path)
    mock_possible_alerts.return_value = ["test_instrument"]

    add_alert_fields_if_needed(sample_csv_path)

    assert "Could not find record ID column" in caplog.text
    assert not mock_get_records.called


@patch("hbnmigration.from_curious.data_to_redcap.time.sleep")
@patch("hbnmigration.from_curious.data_to_redcap.get_alert_field_event")
@patch("hbnmigration.from_curious.data_to_redcap.possible_alert_instruments")
def test_add_alert_fields_adds_columns_for_alert_instruments(
    mock_possible_alerts, mock_get_event, mock_sleep, sample_csv_path
):
    """Test that alert field columns are added to CSV for alert instruments."""
    df = pl.DataFrame({"record_id": ["001", "002"], "field1": [1, 2]})
    df.write_csv(sample_csv_path)
    mock_possible_alerts.return_value = ["test_instrument"]
    mock_get_event.return_value = "admin_arm_1"

    add_alert_fields_if_needed(sample_csv_path)

    # Read the updated CSV
    result = pl.read_csv(sample_csv_path)
    # Should have alert field and event column added
    assert "test_instrument_alerts" in result.columns
    assert "redcap_event_name" in result.columns


@patch("hbnmigration.from_curious.data_to_redcap.time.sleep")
@patch("hbnmigration.from_curious.data_to_redcap.get_alert_field_event")
@patch("hbnmigration.from_curious.data_to_redcap.possible_alert_instruments")
def test_add_alert_fields_sets_no_for_new_records(
    mock_possible_alerts, mock_get_event, mock_sleep, sample_csv_path
):
    """Test that alert field is populated for records."""
    df = pl.DataFrame({"record_id": ["001", "002"], "field1": [1, 2]})
    df.write_csv(sample_csv_path)
    mock_possible_alerts.return_value = ["test_instrument"]
    mock_get_event.return_value = "admin_arm_1"

    add_alert_fields_if_needed(sample_csv_path)

    # Read the updated CSV
    result = pl.read_csv(sample_csv_path)
    # Should have alert field
    assert "test_instrument_alerts" in result.columns
    # At least some values should be 'no' or None (None represents empty from merge)
    alert_values = result["test_instrument_alerts"].to_list()
    assert any(v == "no" or v is None for v in alert_values)


@patch("hbnmigration.from_curious.data_to_redcap.time.sleep")
@patch("hbnmigration.from_curious.data_to_redcap.get_alert_field_event")
@patch("hbnmigration.from_curious.data_to_redcap.possible_alert_instruments")
def test_add_alert_fields_waits_before_checking_redcap(
    mock_possible_alerts, mock_get_event, mock_sleep, sample_csv_path
):
    """Test that function waits 15 seconds before checking REDCap."""
    df = pl.DataFrame({"record_id": ["001"], "field1": [1]})
    df.write_csv(sample_csv_path)
    mock_possible_alerts.return_value = ["test_instrument"]
    mock_get_event.return_value = "admin_arm_1"

    add_alert_fields_if_needed(sample_csv_path)

    # Verify sleep was called with 15 seconds
    mock_sleep.assert_called_with(15)


@patch("hbnmigration.from_curious.data_to_redcap.time.sleep")
@patch("hbnmigration.from_curious.data_to_redcap.get_alert_field_event")
@patch("hbnmigration.from_curious.data_to_redcap.possible_alert_instruments")
def test_add_alert_fields_uses_event_column_if_present(
    mock_possible_alerts, mock_get_event, mock_sleep, sample_csv_path
):
    """Test that existing event column is used if present."""
    df = pl.DataFrame(
        {
            "record_id": ["001", "002"],
            "redcap_event_name": ["baseline_arm_1", "followup_arm_1"],
            "field1": [1, 2],
        }
    )
    df.write_csv(sample_csv_path)
    mock_possible_alerts.return_value = ["test_instrument"]
    mock_get_event.return_value = "admin_arm_1"

    add_alert_fields_if_needed(sample_csv_path)

    result = pl.read_csv(sample_csv_path)
    # Check that the redcap_event_name column still exists
    assert "redcap_event_name" in result.columns
    # Alert field should be added
    assert "test_instrument_alerts" in result.columns


# ============================================================================
# Tests - split_csv_by_fields
# ============================================================================


@pytest.fixture
def sample_csv_with_mixed_fields(tmp_path):
    """Create CSV with both valid and unfound fields."""
    csv_path = tmp_path / "test_instrument.csv"
    df = pl.DataFrame(
        {
            "record_id": ["001", "002"],
            "redcap_event_name": ["baseline_arm_1", "baseline_arm_1"],
            "valid_field_1": ["a", "b"],
            "valid_field_2": [1, 2],
            "unfound_field_1": ["x", "y"],
            "unfound_field_2": [10, 20],
        }
    )
    df.write_csv(csv_path)
    return csv_path


def test_split_csv_by_fields_creates_two_files(
    sample_csv_with_mixed_fields, mock_config_log_root
):
    """Test that split_csv_by_fields creates valid and unfound files."""
    from hbnmigration.from_curious.data_to_redcap import split_csv_by_fields

    unfound_fields = ["unfound_field_1", "unfound_field_2"]
    valid_path, unfound_path = split_csv_by_fields(
        sample_csv_with_mixed_fields, unfound_fields
    )

    # Check both files exist
    assert valid_path.exists()
    assert unfound_path
    assert unfound_path.exists()

    # Check unfound file is in LOG_ROOT
    assert mock_config_log_root in unfound_path.parents
    assert "unfound_fields" in str(unfound_path)


def test_split_csv_valid_file_excludes_unfound_fields(
    sample_csv_with_mixed_fields, mock_config_log_root
):
    """Test that valid file excludes unfound fields."""
    from hbnmigration.from_curious.data_to_redcap import split_csv_by_fields

    unfound_fields = ["unfound_field_1", "unfound_field_2"]
    valid_path, _ = split_csv_by_fields(sample_csv_with_mixed_fields, unfound_fields)

    valid_df = pl.read_csv(valid_path)

    # Should have identifiers and valid fields only
    assert "record_id" in valid_df.columns
    assert "redcap_event_name" in valid_df.columns
    assert "valid_field_1" in valid_df.columns
    assert "valid_field_2" in valid_df.columns

    # Should NOT have unfound fields
    assert "unfound_field_1" not in valid_df.columns
    assert "unfound_field_2" not in valid_df.columns


def test_split_csv_unfound_file_includes_identifiers(
    sample_csv_with_mixed_fields, mock_config_log_root
):
    """Test that unfound file includes record identifiers."""
    from hbnmigration.from_curious.data_to_redcap import split_csv_by_fields

    unfound_fields = ["unfound_field_1", "unfound_field_2"]
    _, unfound_path = split_csv_by_fields(sample_csv_with_mixed_fields, unfound_fields)

    unfound_df = pl.read_csv(unfound_path)

    # Should have identifiers
    assert "record_id" in unfound_df.columns
    assert "redcap_event_name" in unfound_df.columns

    # Should have unfound fields
    assert "unfound_field_1" in unfound_df.columns
    assert "unfound_field_2" in unfound_df.columns

    # Should NOT have valid fields
    assert "valid_field_1" not in unfound_df.columns
    assert "valid_field_2" not in unfound_df.columns


def test_split_csv_unfound_file_has_timestamp(
    sample_csv_with_mixed_fields, mock_config_log_root
):
    """Test that unfound file has timestamp in filename."""
    import re

    from hbnmigration.from_curious.data_to_redcap import split_csv_by_fields

    unfound_fields = ["unfound_field_1"]
    _, unfound_path = split_csv_by_fields(sample_csv_with_mixed_fields, unfound_fields)

    # Check filename contains timestamp pattern (YYYYMMDD_HHMMSS)
    timestamp_pattern = r"\d{8}_\d{6}"
    assert re.search(timestamp_pattern, unfound_path.name)


def test_split_csv_no_unfound_fields_present(
    sample_csv_with_mixed_fields, mock_config_log_root, caplog
):
    """Test split_csv when unfound fields aren't in the CSV."""
    from hbnmigration.from_curious.data_to_redcap import split_csv_by_fields

    unfound_fields = ["field_that_doesnt_exist_1", "field_that_doesnt_exist_2"]
    valid_path, unfound_path = split_csv_by_fields(
        sample_csv_with_mixed_fields, unfound_fields
    )

    # Should return original path and None
    assert valid_path == sample_csv_with_mixed_fields
    assert unfound_path is None
    assert "None of the unfound fields are present" in caplog.text


def test_split_csv_preserves_all_data_rows(
    sample_csv_with_mixed_fields, mock_config_log_root
):
    """Test that split doesn't lose any data rows."""
    from hbnmigration.from_curious.data_to_redcap import split_csv_by_fields

    original_df = pl.read_csv(sample_csv_with_mixed_fields)
    unfound_fields = ["unfound_field_1", "unfound_field_2"]

    valid_path, unfound_path = split_csv_by_fields(
        sample_csv_with_mixed_fields, unfound_fields
    )

    valid_df = pl.read_csv(valid_path)
    unfound_df = pl.read_csv(unfound_path)

    # Both should have same number of rows
    assert len(valid_df) == len(original_df)
    assert len(unfound_df) == len(original_df)

    # Record IDs should match
    assert set(valid_df["record_id"]) == set(original_df["record_id"])
    assert set(unfound_df["record_id"]) == set(original_df["record_id"])


# ============================================================================
# Tests - extract_unfound_fields
# ============================================================================


def test_extract_unfound_fields_parses_error_message():
    """Test extract_unfound_fields parses REDCap error correctly."""
    from hbnmigration.from_curious.data_to_redcap import extract_unfound_fields

    error_text = (
        "ERROR: The following fields were not found in the project as real data fields:"
        " field1, field2, field3"
    )

    result = extract_unfound_fields(error_text)
    assert result == ["field1", "field2", "field3"]


def test_extract_unfound_fields_handles_newline():
    """Test extract_unfound_fields stops at newline."""
    from hbnmigration.from_curious.data_to_redcap import extract_unfound_fields

    error_text = (
        "ERROR: The following fields were not found in the project as real data fields:"
        " field1, field2\nOther error text"
    )

    result = extract_unfound_fields(error_text)
    assert result == ["field1", "field2"]


def test_extract_unfound_fields_strips_whitespace():
    """Test extract_unfound_fields strips whitespace from field names."""
    from hbnmigration.from_curious.data_to_redcap import extract_unfound_fields

    error_text = (
        "ERROR: The following fields were not found in the project as real data fields:"
        " field1 , field2  ,  field3"
    )

    result = extract_unfound_fields(error_text)
    assert result == ["field1", "field2", "field3"]


def test_extract_unfound_fields_no_match():
    """Test extract_unfound_fields returns empty list when no match."""
    from hbnmigration.from_curious.data_to_redcap import extract_unfound_fields

    error_text = "Some other error message"

    result = extract_unfound_fields(error_text)
    assert result == []


def test_extract_unfound_fields_multiline_error():
    """Test extract_unfound_fields with multiline error message."""
    from hbnmigration.from_curious.data_to_redcap import extract_unfound_fields

    error_text = """
    HTTP Error 400
    ERROR: The following fields were not found in the project as real data fields: pmhs_p_start_date, pmhs_p_total_score

    Additional context
    """  # noqa: E501

    result = extract_unfound_fields(error_text)
    assert result == ["pmhs_p_start_date", "pmhs_p_total_score"]


# ============================================================================
# Tests - push_to_redcap with retry logic
# ============================================================================


@patch("hbnmigration.from_curious.data_to_redcap.requests.post")
@patch("hbnmigration.from_curious.data_to_redcap.add_alert_fields_if_needed")
def test_push_to_redcap_success_no_retry(
    mock_add_alerts, mock_post, sample_csv_with_mixed_fields
):
    """Test push_to_redcap succeeds without retry."""
    from hbnmigration.from_curious.data_to_redcap import push_to_redcap

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_post.return_value = mock_response

    push_to_redcap(sample_csv_with_mixed_fields)

    assert mock_post.call_count == 1
    assert mock_add_alerts.called


@patch("hbnmigration.from_curious.data_to_redcap.split_csv_by_fields")
@patch("hbnmigration.from_curious.data_to_redcap.requests.post")
@patch("hbnmigration.from_curious.data_to_redcap.add_alert_fields_if_needed")
def test_push_to_redcap_retries_on_field_error(
    mock_add_alerts,
    mock_post,
    mock_split,
    sample_csv_with_mixed_fields,
    tmp_path,
    mock_config_log_root,
):
    """Test push_to_redcap retries after splitting on field error."""
    from hbnmigration.from_curious.data_to_redcap import push_to_redcap

    # First call fails with field error
    mock_error_response = MagicMock()
    mock_error_response.status_code = 400
    mock_error_response.text = (
        "ERROR: The following fields were not found in the project as real data fields:"
        " unfound_field_1, unfound_field_2"
    )

    # Second call succeeds
    mock_success_response = MagicMock()
    mock_success_response.status_code = 200

    mock_post.side_effect = [mock_error_response, mock_success_response]

    # Mock split to return valid path
    valid_path = tmp_path / "valid.csv"
    pl.DataFrame({"record_id": ["001"], "valid_field": ["a"]}).write_csv(valid_path)
    unfound_path = mock_config_log_root / "unfound_fields" / "unfound.csv"
    mock_split.return_value = (valid_path, unfound_path)

    push_to_redcap(sample_csv_with_mixed_fields)

    # Should have called post twice (original + retry)
    assert mock_post.call_count == 2
    assert mock_split.called


@patch("hbnmigration.from_curious.data_to_redcap.requests.post")
@patch("hbnmigration.from_curious.data_to_redcap.add_alert_fields_if_needed")
def test_push_to_redcap_no_retry_on_other_errors(
    mock_add_alerts, mock_post, sample_csv_with_mixed_fields
):
    """Test push_to_redcap doesn't retry on non-field errors."""
    from hbnmigration.from_curious.data_to_redcap import push_to_redcap

    mock_response = MagicMock()
    mock_response.status_code = 401
    mock_response.text = "Unauthorized"
    mock_response.raise_for_status.side_effect = Exception("401 Unauthorized")
    mock_post.return_value = mock_response

    with pytest.raises(Exception):
        push_to_redcap(sample_csv_with_mixed_fields)

    # Should only try once
    assert mock_post.call_count == 1


@patch("hbnmigration.from_curious.data_to_redcap.split_csv_by_fields")
@patch("hbnmigration.from_curious.data_to_redcap.requests.post")
@patch("hbnmigration.from_curious.data_to_redcap.add_alert_fields_if_needed")
def test_push_to_redcap_logs_unfound_path(
    mock_add_alerts,
    mock_post,
    mock_split,
    sample_csv_with_mixed_fields,
    mock_config_log_root,
    caplog,
):
    """Test push_to_redcap logs the unfound fields file path."""
    from hbnmigration.from_curious.data_to_redcap import push_to_redcap

    mock_error_response = MagicMock()
    mock_error_response.status_code = 400
    mock_error_response.text = (
        "ERROR: The following fields were not found in the project as real data fields:"
        " unfound_field_1"
    )

    mock_success_response = MagicMock()
    mock_success_response.status_code = 200

    mock_post.side_effect = [mock_error_response, mock_success_response]

    unfound_path = mock_config_log_root / "unfound_fields" / "test_20240101_120000.csv"
    unfound_path.parent.mkdir(parents=True, exist_ok=True)
    unfound_path.touch()

    mock_split.return_value = (sample_csv_with_mixed_fields, unfound_path)

    push_to_redcap(sample_csv_with_mixed_fields)

    assert "Unfound fields data saved to:" in caplog.text
    assert str(unfound_path) in caplog.text


@patch("hbnmigration.from_curious.data_to_redcap.add_alert_fields_if_needed")
def test_push_to_redcap_skips_empty_file(mock_add_alerts, tmp_path):
    """Test push_to_redcap skips empty files."""
    from hbnmigration.from_curious.data_to_redcap import push_to_redcap

    empty_csv = tmp_path / "empty.csv"
    empty_csv.touch()

    push_to_redcap(empty_csv)

    # Should not try to add alerts for empty file
    assert not mock_add_alerts.called


# ============================================================================
# Tests - format_for_redcap parent subject filtering
# ============================================================================


def test_format_for_redcap_filters_parent_subjects(
    named_output_with_mixed_subjects, caplog
):
    """Test that format_for_redcap filters out parent subject records (_P suffix)."""
    from mindlogger_data_export.outputs import NamedOutput

    # Simulate the filtering logic that happens in format_for_redcap
    outputs = [named_output_with_mixed_subjects]
    filtered_outputs = []

    for output in outputs:
        df = output.output
        if "target_user_secret_id" in df.columns:
            df_filtered = df.filter(
                ~pl.col("target_user_secret_id").cast(pl.Utf8).str.ends_with("_P")
            )
            filtered_count = len(df) - len(df_filtered)
            if filtered_count > 0:
                filtered_outputs.append(
                    NamedOutput(name=output.name, output=df_filtered)
                )
            else:
                filtered_outputs.append(output)
        else:
            filtered_outputs.append(output)

    # Verify filtering occurred
    assert len(filtered_outputs) == 1
    result_df = filtered_outputs[0].output

    # Should have only 2 rows (child subjects), not 4
    assert len(result_df) == 2
    assert result_df["target_user_secret_id"].to_list() == ["12345", "12346"]


def test_format_for_redcap_preserves_non_parent_subjects(
    named_output_all_child_subjects,
):
    """Test that format_for_redcap preserves records with non-parent subjects."""
    from mindlogger_data_export.outputs import NamedOutput

    outputs = [named_output_all_child_subjects]
    filtered_outputs = []

    for output in outputs:
        df = output.output
        if "target_user_secret_id" in df.columns:
            df_filtered = df.filter(
                ~pl.col("target_user_secret_id").cast(pl.Utf8).str.ends_with("_P")
            )
            filtered_outputs.append(NamedOutput(name=output.name, output=df_filtered))
        else:
            filtered_outputs.append(output)

    # All rows should be preserved
    result_df = filtered_outputs[0].output
    assert len(result_df) == 3
    assert result_df["target_user_secret_id"].to_list() == ["12345", "12346", "12347"]


def test_format_for_redcap_removes_all_parent_subjects(
    named_output_all_parent_subjects,
):
    """Test that format_for_redcap removes all records when all are parent subjects."""
    from mindlogger_data_export.outputs import NamedOutput

    outputs = [named_output_all_parent_subjects]
    filtered_outputs = []

    for output in outputs:
        df = output.output
        if "target_user_secret_id" in df.columns:
            df_filtered = df.filter(
                ~pl.col("target_user_secret_id").cast(pl.Utf8).str.ends_with("_P")
            )
            filtered_outputs.append(NamedOutput(name=output.name, output=df_filtered))
        else:
            filtered_outputs.append(output)

    # All rows should be removed
    result_df = filtered_outputs[0].output
    assert len(result_df) == 0


def test_format_for_redcap_handles_missing_target_column(
    named_output_no_target_column,
):
    """Test that format_for_redcap works without target_user_secret_id column."""
    from mindlogger_data_export.outputs import NamedOutput

    outputs = [named_output_no_target_column]
    filtered_outputs = []

    for output in outputs:
        df = output.output
        if "target_user_secret_id" in df.columns:
            df_filtered = df.filter(
                ~pl.col("target_user_secret_id").cast(pl.Utf8).str.ends_with("_P")
            )
            filtered_outputs.append(NamedOutput(name=output.name, output=df_filtered))
        else:
            # If no target_user_secret_id, keep the output as-is
            filtered_outputs.append(output)

    # Output should be preserved when no target column exists
    result_df = filtered_outputs[0].output
    assert len(result_df) == 2
    assert "target_user_secret_id" not in result_df.columns


def test_format_for_redcap_filters_numeric_target_ids():
    """Test that filtering works with numeric target_user_secret_id values."""
    from mindlogger_data_export.outputs import NamedOutput

    df = pl.DataFrame(
        {
            "record_id": ["001", "002", "003"],
            "target_user_secret_id": [12345, 12346, 12347],  # Numeric values
            "activity_name": ["baseline", "baseline", "baseline"],
        }
    )
    output = NamedOutput(name="test_activity_redcap", output=df)
    outputs = [output]
    filtered_outputs = []

    for output in outputs:
        df = output.output
        if "target_user_secret_id" in df.columns:
            df_filtered = df.filter(
                ~pl.col("target_user_secret_id").cast(pl.Utf8).str.ends_with("_P")
            )
            filtered_outputs.append(NamedOutput(name=output.name, output=df_filtered))
        else:
            filtered_outputs.append(output)

    # No rows should be filtered (no _P suffix)
    result_df = filtered_outputs[0].output
    assert len(result_df) == 3


def test_format_for_redcap_filters_string_numeric_with_p_suffix():
    """Test that filtering works with string numeric IDs with _P suffix."""
    from mindlogger_data_export.outputs import NamedOutput

    df = pl.DataFrame(
        {
            "record_id": ["001", "002", "003", "004"],
            "target_user_secret_id": ["12345", "12346_P", "12347_P", "12348"],
            "activity_name": ["baseline", "baseline", "baseline", "baseline"],
        }
    )
    output = NamedOutput(name="test_activity_redcap", output=df)
    outputs = [output]
    filtered_outputs = []

    for output in outputs:
        df = output.output
        if "target_user_secret_id" in df.columns:
            df_filtered = df.filter(
                ~pl.col("target_user_secret_id").cast(pl.Utf8).str.ends_with("_P")
            )
            filtered_outputs.append(NamedOutput(name=output.name, output=df_filtered))
        else:
            filtered_outputs.append(output)

    # Should filter 2 rows with _P suffix
    result_df = filtered_outputs[0].output
    assert len(result_df) == 2
    assert result_df["target_user_secret_id"].to_list() == ["12345", "12348"]
