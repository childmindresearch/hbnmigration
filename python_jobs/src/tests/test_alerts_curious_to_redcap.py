"""Tests for alerts_to_redcap module."""

from contextlib import contextmanager
import json
from unittest.mock import AsyncMock, Mock, patch

import pandas as pd
import pytest

from hbnmigration.from_curious.alerts_to_redcap import (
    cli,
    main,
    process_alerts_for_redcap,
    push_alerts_to_redcap,
    synchronous_main,
    toggle_alerts,
)
from hbnmigration.from_redcap.from_redcap import response_index_reverse_lookup

# ============================================================================
# Test Data Factories
# ============================================================================


def create_alert_df(
    records: list[str],
    field_names: list[str],
    values: list[str],
    events: list[str] | None = None,
) -> pd.DataFrame:
    """Create alert DataFrames."""
    data = {
        "record": records,
        "field_name": field_names,
        "value": values,
    }
    if events:
        data["redcap_event_name"] = events
    return pd.DataFrame(data)


def create_empty_alert_df() -> pd.DataFrame:
    """Create empty alert DataFrame."""
    return pd.DataFrame(
        {
            "record": pd.Series([], dtype=str),
            "field_name": pd.Series([], dtype=str),
            "value": pd.Series([], dtype=str),
            "redcap_event_name": pd.Series([], dtype=str),
        }
    )


def create_minimal_metadata() -> pd.DataFrame:
    """Create minimal alert metadata."""
    return pd.DataFrame(
        {
            "field_name": ["mrn", "alerts_parent_baseline_1"],
            "select_choices_or_calculations": ["", "0, No | 1, Yes"],
        }
    )


def create_minimal_existing_data() -> pd.DataFrame:
    """Create minimal existing REDCap data."""
    return pd.DataFrame(
        {
            "record": ["1"],
            "field_name": ["mrn"],
            "value": ["12345"],
            "redcap_event_name": ["baseline_arm_1"],
        }
    )


def create_mock_tokens(auth_token: str = "test_token") -> Mock:
    """Create mock authentication tokens."""
    mock_tokens = Mock()
    mock_tokens.access = auth_token
    mock_tokens.endpoints = Mock()
    mock_tokens.endpoints.alerts = "https://curious.test/alerts"
    return mock_tokens


# ============================================================================
# Tests - toggle_alerts
# ============================================================================


def test_toggle_alerts_creates_summary_rows():
    """Test that toggle_alerts creates instrument-level alert flags."""
    df = create_alert_df(
        ["12345", "12345", "67890"],
        [
            "alerts_parent_baseline_1",
            "alerts_child_baseline_2",
            "alerts_parent_followup_1",
        ],
        ["Yes", "Sometimes", "No"],
        ["baseline_arm_1", "baseline_arm_1", "followup_arm_1"],
    )
    result = toggle_alerts(df)
    assert len(result) > len(df)
    summary_fields = result[result["field_name"].str.endswith("_alerts")]
    assert len(summary_fields) > 0
    assert all(summary_fields["value"] == "yes")


def test_toggle_alerts_extracts_instrument_names():
    """Test that instrument names are correctly extracted."""
    df = create_alert_df(
        ["12345", "12345", "67890"],
        [
            "alerts_parent_baseline_1",
            "alerts_child_baseline_2",
            "alerts_parent_followup_1",
        ],
        ["Yes", "Sometimes", "No"],
        ["baseline_arm_1", "baseline_arm_1", "followup_arm_1"],
    )
    result = toggle_alerts(df)
    summary_rows = result[result["field_name"].str.endswith("_alerts")]
    expected = {
        "parent_baseline_alerts",
        "child_baseline_alerts",
        "parent_followup_alerts",
    }
    assert set(summary_rows["field_name"].unique()).issubset(expected)


def test_toggle_alerts_deduplicates_summary_rows():
    """Test that duplicate summary rows are removed."""
    df = create_alert_df(
        ["001", "001", "001"],
        [
            "alerts_parent_baseline_1",
            "alerts_parent_baseline_2",
            "alerts_parent_baseline_3",
        ],
        ["Yes", "No", "Sometimes"],
        ["baseline_arm_1"] * 3,
    )
    result = toggle_alerts(df)
    summary_rows = result[
        (result["field_name"] == "parent_baseline_alerts") & (result["record"] == "001")
    ]
    assert len(summary_rows) == 1


def test_toggle_alerts_preserves_original_data():
    """Test that original alert data is preserved."""
    df = create_alert_df(
        ["12345", "12345", "67890"],
        [
            "alerts_parent_baseline_1",
            "alerts_child_baseline_2",
            "alerts_parent_followup_1",
        ],
        ["Yes", "Sometimes", "No"],
        ["baseline_arm_1", "baseline_arm_1", "followup_arm_1"],
    )
    original_records = set(df["record"].unique())
    original_fields = set(df["field_name"].unique())
    result = toggle_alerts(df)
    assert original_records.issubset(set(result["record"].unique()))
    assert original_fields.issubset(set(result["field_name"].unique()))


def test_toggle_alerts_handles_empty_dataframe():
    """Test that empty DataFrame is handled gracefully."""
    empty_df = create_empty_alert_df()
    result = toggle_alerts(empty_df)
    assert len(result) == 0
    assert list(result.columns) == list(empty_df.columns)


def test_toggle_alerts_kevin_urgent_alert(kevin_alert):
    """Test Kevin's urgent alert creates proper summary."""
    df = create_alert_df(
        ["005"], ["alerts_parent_baseline_5"], ["Urgent"], ["baseline_arm_1"]
    )
    result = toggle_alerts(df)
    summary_row = result[result["field_name"] == "parent_baseline_alerts"]
    assert len(summary_row) == 1
    assert summary_row["value"].iloc[0] == "yes"
    assert summary_row["record"].iloc[0] == "005"


# ============================================================================
# Tests - process_alerts_for_redcap
# ============================================================================


def test_process_alerts_fetches_metadata(
    mock_alerts_dependencies,
    redcap_alert_df,
    redcap_alerts_metadata,
    redcap_existing_alert_data,
):
    """Test that metadata is fetched for alerts instrument."""
    mocks = mock_alerts_dependencies
    mocks["fetch_metadata"].return_value = redcap_alerts_metadata
    mocks["fetch"].return_value = redcap_existing_alert_data

    process_alerts_for_redcap(redcap_alert_df)

    assert mocks["fetch_metadata"].called


def test_process_alerts_calls_required_functions(mock_alerts_dependencies):
    """Test that process_alerts_for_redcap calls the necessary functions."""
    mocks = mock_alerts_dependencies
    mocks["fetch_metadata"].return_value = create_minimal_metadata()
    mocks["fetch"].return_value = create_minimal_existing_data()
    alert_data = create_alert_df(["12345"], ["alerts_parent_baseline_1"], ["yes"])

    result = process_alerts_for_redcap(alert_data)

    assert isinstance(result, pd.DataFrame)
    assert mocks["fetch_metadata"].called
    assert mocks["fetch"].called


def test_process_alerts_returns_dataframe_with_required_columns(
    mock_alerts_dependencies,
):
    """Test that result has required REDCap columns."""
    mocks = mock_alerts_dependencies
    mocks["fetch_metadata"].return_value = create_minimal_metadata()
    mocks["fetch"].return_value = create_minimal_existing_data()
    alert_data = create_alert_df(["12345"], ["alerts_parent_baseline_1"], ["yes"])

    result = process_alerts_for_redcap(alert_data)

    for col in ["record", "field_name", "value"]:
        assert col in result.columns


def test_process_alerts_toggles_summary_flags(
    mock_alerts_dependencies, redcap_alerts_metadata
):
    """Test that instrument-level alert flags are toggled."""
    mocks = mock_alerts_dependencies
    mocks["fetch_metadata"].return_value = redcap_alerts_metadata
    mocks["fetch"].return_value = create_alert_df(
        ["1", "1"],
        ["mrn", "alerts_parent_baseline_1"],
        ["12345", "0"],
        ["baseline_arm_1", "baseline_arm_1"],
    )
    alert_data = create_alert_df(["MRN12345"], ["alerts_parent_baseline_1"], ["yes"])

    result = process_alerts_for_redcap(alert_data)

    if len(result) > 0:
        summary_rows = result[result["field_name"].str.endswith("_alerts")]
        if len(summary_rows) > 0:
            assert all(summary_rows["value"] == "yes")


def test_process_alerts_partial_redcap_landing(
    mock_alerts_dependencies, redcap_alerts_metadata
):
    """Test partial mode filters to only fields present in REDCap."""
    mocks = mock_alerts_dependencies
    mocks["fetch_metadata"].return_value = redcap_alerts_metadata
    mocks["fetch"].return_value = create_alert_df(
        ["1", "1"],
        ["mrn", "alerts_parent_baseline_1"],
        ["12345", "0"],
        ["baseline_arm_1", "baseline_arm_1"],
    )
    alert_df = create_alert_df(
        ["MRN12345", "MRN12345"],
        ["alerts_parent_baseline_1", "alerts_nonexistent_field"],
        ["yes", "Yes"],
    )

    result = process_alerts_for_redcap(alert_df, partial_redcap_landing=True)

    assert "alerts_nonexistent_field" not in result["field_name"].values


def test_process_alerts_handles_missing_mrn(
    mock_alerts_dependencies, redcap_alerts_metadata
):
    """Test handling of alerts with MRN not found in REDCap."""
    mocks = mock_alerts_dependencies
    mocks["fetch_metadata"].return_value = redcap_alerts_metadata
    mocks["fetch"].return_value = create_alert_df(
        ["1", "1"],
        ["mrn", "alerts_parent_baseline_1"],
        ["12345", "0"],
        ["baseline_arm_1", "baseline_arm_1"],
    )
    alert_df = create_alert_df(["MRN99999"], ["alerts_parent_baseline_1"], ["yes"])

    result = process_alerts_for_redcap(alert_df)

    assert len(result) == 0 or result["record"].isna().all()


# ============================================================================
# Tests - push_alerts_to_redcap
# ============================================================================


def test_push_alerts_calls_api(mock_alerts_dependencies, processed_alerts_for_push):
    """Test that push_alerts_to_redcap calls the API."""
    mocks = mock_alerts_dependencies
    push_alerts_to_redcap(processed_alerts_for_push)
    assert mocks["push"].called
    assert len(mocks["push"].call_args[0][0]) == len(processed_alerts_for_push)


def test_push_alerts_logs_success(
    mock_alerts_dependencies, processed_alerts_for_push, caplog
):
    """Test that successful push logs appropriate message."""
    push_alerts_to_redcap(processed_alerts_for_push)
    assert "successfully updated" in caplog.text
    assert "PID 625" in caplog.text


def test_push_alerts_handles_api_error(
    mock_alerts_dependencies, processed_alerts_for_push
):
    """Test that API errors are raised appropriately."""
    mocks = mock_alerts_dependencies
    mocks["push"].side_effect = Exception("API Error")
    with pytest.raises(Exception, match="API Error"):
        push_alerts_to_redcap(processed_alerts_for_push)


def test_push_alerts_kevin_urgent(mock_alerts_dependencies):
    """Test pushing Kevin's urgent alert."""
    mocks = mock_alerts_dependencies
    kevin_alerts = create_alert_df(
        ["005", "005"],
        ["alerts_parent_baseline_5", "parent_baseline_alerts"],
        ["2", "yes"],
        ["baseline_arm_1", "baseline_arm_1"],
    )
    push_alerts_to_redcap(kevin_alerts)
    assert mocks["push"].called
    pushed_data = mocks["push"].call_args[0][0]
    assert len(pushed_data) == 2
    assert "2" in pushed_data["value"].values


# ============================================================================
# Test Helpers for Main Functions
# ============================================================================


@contextmanager
def _setup_main_test_mocks(
    mock_alerts_dependencies,
    sample_alert=None,
    existing_data=None,
    parse_return=None,
    metadata_return=None,
):
    """Context manager for common main test setup."""
    mocks = mock_alerts_dependencies
    mocks["fetch_metadata"].return_value = (
        metadata_return
        if metadata_return is not None
        else pd.DataFrame(
            {
                "field_name": ["mrn", "alerts_parent_baseline_1"],
                "select_choices_or_calculations": ["", "0, No | 1, Yes"],
            }
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

    with (
        patch(
            "hbnmigration.from_curious.alerts_to_redcap._curious_authenticate"
        ) as mock_auth,
        patch(
            "hbnmigration.from_curious.alerts_to_redcap.connect_to_websocket"
        ) as mock_ws,
        patch("hbnmigration.from_curious.alerts_to_redcap.parse_alert") as mock_parse,
    ):
        mock_auth.return_value = create_mock_tokens()
        mock_websocket = AsyncMock()

        if sample_alert is not None:
            mock_websocket.__aiter__.return_value = [json.dumps(sample_alert)]

        mock_ws.return_value.__aenter__.return_value = mock_websocket

        if parse_return is not None:
            mock_parse.return_value = parse_return

        yield {"auth": mock_auth, "ws": mock_ws, "parse": mock_parse}


@contextmanager
def _setup_sync_main_mocks(
    mock_alerts_dependencies,
    alerts_list,
    parse_returns,
    metadata_return=None,
    status_code=200,
):
    """Set up synchronous main test mocks."""
    mocks = mock_alerts_dependencies
    mocks["fetch_metadata"].return_value = (
        metadata_return
        if metadata_return is not None
        else pd.DataFrame(
            {
                "field_name": ["mrn", "alerts_parent_baseline_1"],
                "select_choices_or_calculations": ["", "0, No | 1, Yes"],
            }
        )
    )

    with (
        patch(
            "hbnmigration.from_curious.alerts_to_redcap._curious_authenticate"
        ) as mock_auth,
        patch("hbnmigration.from_curious.alerts_to_redcap.requests.get") as mock_get,
        patch("hbnmigration.from_curious.alerts_to_redcap.parse_alert") as mock_parse,
    ):
        mock_auth.return_value = create_mock_tokens()
        mock_response = Mock()
        mock_response.status_code = status_code
        if status_code == 200:
            mock_response.json.return_value = {"result": alerts_list}
        mock_get.return_value = mock_response

        if parse_returns is not None:
            mock_parse.side_effect = parse_returns

        yield {"auth": mock_auth, "get": mock_get, "parse": mock_parse}


# ============================================================================
# Tests - Integration (Async Main)
# ============================================================================


@pytest.mark.asyncio
async def test_main_processes_websocket_messages(
    mock_alerts_dependencies, sample_curious_alert, redcap_alerts_metadata
):
    """Test that main() processes websocket messages."""
    mocks = mock_alerts_dependencies
    mocks["fetch_metadata"].return_value = redcap_alerts_metadata
    mocks["fetch"].return_value = create_alert_df(
        ["1", "1"],
        ["mrn", "alerts_parent_baseline_1"],
        ["12345", "0"],
        ["baseline_arm_1", "baseline_arm_1"],
    )

    with _setup_main_test_mocks(
        mock_alerts_dependencies,
        sample_alert=sample_curious_alert,
        parse_return=create_alert_df(
            ["MRN12345"], ["alerts_parent_baseline_1"], ["yes"]
        ),
        metadata_return=redcap_alerts_metadata,
    ) as test_mocks:
        await main()
        assert test_mocks["parse"].called


@pytest.mark.asyncio
async def test_main_skips_non_answer_messages(mock_alerts_dependencies):
    """Test that main() skips non-answer message types."""
    mocks = mock_alerts_dependencies
    non_answer = {"type": "heartbeat", "timestamp": "2024-01-01T00:00:00Z"}

    with _setup_main_test_mocks(mock_alerts_dependencies, sample_alert=non_answer):
        await main()
        assert not mocks["push"].called


@pytest.mark.asyncio
async def test_main_handles_json_decode_error(mock_alerts_dependencies, caplog):
    """Test that main() handles malformed JSON gracefully."""
    mocks = mock_alerts_dependencies

    with (
        patch(
            "hbnmigration.from_curious.alerts_to_redcap._curious_authenticate"
        ) as mock_auth,
        patch(
            "hbnmigration.from_curious.alerts_to_redcap.connect_to_websocket"
        ) as mock_ws,
    ):
        mock_auth.return_value = create_mock_tokens()
        mock_websocket = AsyncMock()
        mock_websocket.__aiter__.return_value = ["invalid json {{{"]
        mock_ws.return_value.__aenter__.return_value = mock_websocket

        await main()
        assert "Failed to parse message as JSON" in caplog.text
        assert not mocks["push"].called


# ============================================================================
# Tests - Integration (Synchronous Main)
# ============================================================================


def test_synchronous_main_fetches_alerts(
    mock_alerts_dependencies, multiple_curious_alerts, redcap_alerts_metadata
):
    """Test that synchronous_main() fetches and processes alerts."""
    mocks = mock_alerts_dependencies
    mocks["fetch_metadata"].return_value = redcap_alerts_metadata
    mocks["fetch"].return_value = create_alert_df(
        ["1", "1", "2", "2"],
        ["mrn", "alerts_parent_baseline_1", "mrn", "alerts_parent_followup_1"],
        ["12345", "0", "67890", ""],
        ["baseline_arm_1", "baseline_arm_1", "followup_arm_1", "followup_arm_1"],
    )

    parse_returns = [
        create_alert_df(["MRN12345"], ["alerts_parent_baseline_1"], ["yes"]),
        create_alert_df(["MRN12345"], ["alerts_child_baseline_1"], ["sometimes"]),
        create_alert_df(["MRN67890"], ["alerts_parent_followup_1"], ["no"]),
    ]

    with _setup_sync_main_mocks(
        mock_alerts_dependencies,
        multiple_curious_alerts,
        parse_returns,
        metadata_return=redcap_alerts_metadata,
    ) as test_mocks:
        synchronous_main()
        assert test_mocks["parse"].call_count == 3


def test_synchronous_main_handles_api_error(mock_alerts_dependencies):
    """Test that synchronous_main() handles API errors gracefully."""
    mocks = mock_alerts_dependencies

    with _setup_sync_main_mocks(mock_alerts_dependencies, [], None, status_code=500):
        synchronous_main()
        assert not mocks["push"].called


def test_synchronous_main_partial_mode(
    mock_alerts_dependencies, redcap_alerts_metadata
):
    """Test synchronous_main with partial_redcap_landing flag."""
    mocks = mock_alerts_dependencies
    mocks["fetch_metadata"].return_value = redcap_alerts_metadata
    mocks["fetch"].return_value = create_alert_df(
        ["1", "1"],
        ["mrn", "alerts_parent_baseline_1"],
        ["12345", "0"],
        ["baseline_arm_1", "baseline_arm_1"],
    )

    alert = [
        {
            "type": "answer",
            "id": "alert_001",
            "activityItemId": "alerts_parent_baseline_1",
        }
    ]
    parse_return = [
        create_alert_df(["MRN12345"], ["alerts_parent_baseline_1"], ["yes"])
    ]

    with _setup_sync_main_mocks(
        mock_alerts_dependencies,
        alert,
        parse_return,
        metadata_return=redcap_alerts_metadata,
    ) as test_mocks:
        synchronous_main(partial_redcap_landing=True)
        assert test_mocks["parse"].called


# ============================================================================
# Tests - CLI
# ============================================================================


def test_cli_async_mode():
    """Test CLI runs in async mode by default."""
    with (
        patch("hbnmigration.from_curious.alerts_to_redcap.asyncio.run") as mock_run,
        patch("sys.argv", ["alerts_to_redcap.py"]),
    ):
        cli()
        assert mock_run.called
        assert mock_run.call_count == 1


@pytest.mark.filterwarnings("ignore:coroutine 'main' was never awaited:RuntimeWarning")
def test_cli_sync_mode():
    """Test CLI runs in sync mode with --synchronous flag."""
    with (
        patch(
            "hbnmigration.from_curious.alerts_to_redcap.synchronous_main"
        ) as mock_sync,
        patch("hbnmigration.from_curious.alerts_to_redcap.main") as mock_main,
        patch("hbnmigration.from_curious.alerts_to_redcap.asyncio.run") as mock_async,
        patch("sys.argv", ["alerts_to_redcap.py", "--synchronous"]),
    ):
        cli()
        assert mock_sync.called
        assert not mock_async.called
        assert not mock_main.called


@pytest.mark.filterwarnings("ignore:coroutine 'main' was never awaited:RuntimeWarning")
def test_cli_partial_mode():
    """Test CLI passes partial flag correctly."""
    with (
        patch(
            "hbnmigration.from_curious.alerts_to_redcap.synchronous_main"
        ) as mock_sync,
        patch("hbnmigration.from_curious.alerts_to_redcap.main") as mock_main,
        patch("hbnmigration.from_curious.alerts_to_redcap.asyncio.run") as mock_async,
        patch("sys.argv", ["alerts_to_redcap.py", "--synchronous", "--partial"]),
    ):
        cli()
        assert mock_sync.called
        assert mock_sync.call_args[0][0] is True
        assert not mock_async.called
        assert not mock_main.called


# ============================================================================
# Tests - response_index_reverse_lookup
# ============================================================================


@pytest.mark.parametrize(
    "field_name,choices,expected",
    [
        ("test_field", "0, No | 1, Yes | 2, Maybe", ("test_field", "no", 0)),
        ("text_field", "", None),
        ("test_field", "invalid_format", None),
        (
            "alerts_parent_baseline_5",
            "0, Normal | 1, Concerning | 2, Urgent",
            ("alerts_parent_baseline_5", "normal", 0),
        ),
    ],
)
def test_response_index_reverse_lookup(field_name, choices, expected):
    """Test response_index_reverse_lookup with various inputs."""
    row = pd.Series(
        {
            "field_name": field_name,
            "select_choices_or_calculations": choices,
        }
    )
    result = response_index_reverse_lookup(row)
    assert result == expected
