"""Tests for alerts_to_redcap module."""

import json
from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest
from websockets.exceptions import ConnectionClosedError, InvalidStatus

from hbnmigration.from_curious.alerts_to_redcap import (
    cli,
    main,
    main_with_reconnect,
    parse_alert,
    process_alerts_for_redcap,
    push_alerts_to_redcap,
    synchronous_main,
    toggle_alerts,
    websocket_listener,
)
from hbnmigration.from_redcap.from_redcap import response_index_reverse_lookup

# Import shared utilities from conftest
from .conftest import (
    create_alert_df,
    create_mock_invalid_status,
    create_mock_tokens_ws,
    setup_cli_mocks,
    setup_main_test_mocks,
    setup_reconnect_mocks,
    setup_standard_alert_mocks,
    setup_sync_main_mocks,
)

# ============================================================================
# Tests - toggle_alerts
# ============================================================================


def test_toggle_alerts_creates_summary_rows(multi_instrument_alert_df):
    """Test that toggle_alerts creates instrument-level alert flags."""
    result = toggle_alerts(multi_instrument_alert_df)
    assert len(result) > len(multi_instrument_alert_df)
    summary_fields = result[result["field_name"].str.endswith("_alerts")]
    assert len(summary_fields) > 0
    assert all(summary_fields["value"] == "yes")


def test_toggle_alerts_extracts_instrument_names(multi_instrument_alert_df):
    """Test that instrument names are correctly extracted."""
    result = toggle_alerts(multi_instrument_alert_df)
    summary_rows = result[result["field_name"].str.endswith("_alerts")]
    expected = {
        "parent_baseline_alerts",
        "child_baseline_alerts",
        "parent_followup_alerts",
    }
    assert set(summary_rows["field_name"].unique()).issubset(expected)


def test_toggle_alerts_preserves_original_data(multi_instrument_alert_df):
    """Test that original alert data is preserved."""
    original_records = set(multi_instrument_alert_df["record"].unique())
    original_fields = set(multi_instrument_alert_df["field_name"].unique())
    result = toggle_alerts(multi_instrument_alert_df)
    assert original_records.issubset(set(result["record"].unique()))
    assert original_fields.issubset(set(result["field_name"].unique()))


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


def test_toggle_alerts_handles_empty_dataframe():
    """Test that empty DataFrame is handled gracefully."""
    empty_df = create_alert_df()
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
# Tests - parse_alert
# ============================================================================


def test_parse_alert_with_secret_id(sample_curious_alert):
    """Test that parse_alert correctly parses an alert with secretId."""
    # Update message to match the expected format: color: "answer" ... item
    alert = sample_curious_alert.copy()
    alert["message"] = 'Red: "Yes" - Difficulty concentrating parent_baseline'
    result = parse_alert(alert)
    assert not result.empty
    assert "record" in result.columns
    assert "field_name" in result.columns
    assert "value" in result.columns
    assert sample_curious_alert["secretId"] in result["record"].values


def test_parse_alert_without_secret_id(sample_curious_alert):
    """Test that parse_alert returns empty DataFrame when secretId is missing."""
    alert = sample_curious_alert.copy()
    alert["message"] = 'Red: "Yes" - Difficulty concentrating parent_baseline'
    del alert["secretId"]
    result = parse_alert(alert)
    assert result.empty
    assert list(result.columns) == [
        "record",
        "field_name",
        "value",
        "redcap_event_name",
    ]


def test_parse_alert_extracts_mrn(sample_curious_alert):
    """Test that parse_alert extracts MRN correctly."""
    alert = sample_curious_alert.copy()
    alert["message"] = 'Red: "Yes" - Difficulty concentrating parent_baseline'
    result = parse_alert(alert)
    mrn_row = result[result["field_name"] == "mrn"]
    assert len(mrn_row) == 1
    assert mrn_row["value"].iloc[0] == sample_curious_alert["secretId"]


def test_parse_alert_converts_message_to_field_name(sample_curious_alert):
    """Test that parse_alert converts message to alert field name."""
    alert = sample_curious_alert.copy()
    alert["message"] = 'Red: "Yes" - Difficulty concentrating parent_baseline'
    result = parse_alert(alert)
    field_rows = result[result["field_name"] != "mrn"]
    assert len(field_rows) > 0
    assert field_rows["field_name"].iloc[0].startswith("alerts_")


# ============================================================================

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
    setup_standard_alert_mocks(
        mock_alerts_dependencies, redcap_alerts_metadata, redcap_existing_alert_data
    )
    process_alerts_for_redcap(redcap_alert_df)
    assert mock_alerts_dependencies["fetch_metadata"].called


def test_process_alerts_calls_required_functions(mock_alerts_dependencies):
    """Test that process_alerts_for_redcap calls the necessary functions."""
    setup_standard_alert_mocks(mock_alerts_dependencies)
    alert_data = create_alert_df(["12345"], ["alerts_parent_baseline_1"], ["yes"])
    result = process_alerts_for_redcap(alert_data)
    assert isinstance(result, pd.DataFrame)
    assert mock_alerts_dependencies["fetch_metadata"].called
    assert mock_alerts_dependencies["fetch"].called


def test_process_alerts_returns_dataframe_with_required_columns(
    mock_alerts_dependencies,
):
    """Test that result has required REDCap columns."""
    setup_standard_alert_mocks(mock_alerts_dependencies)
    alert_data = create_alert_df(["12345"], ["alerts_parent_baseline_1"], ["yes"])
    result = process_alerts_for_redcap(alert_data)
    for col in ["record", "field_name", "value"]:
        assert col in result.columns


def test_process_alerts_toggles_summary_flags(
    mock_alerts_dependencies, redcap_alerts_metadata
):
    """Test that instrument-level alert flags are toggled."""
    setup_standard_alert_mocks(mock_alerts_dependencies, redcap_alerts_metadata)
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
    setup_standard_alert_mocks(mock_alerts_dependencies, redcap_alerts_metadata)
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
    setup_standard_alert_mocks(mock_alerts_dependencies, redcap_alerts_metadata)
    alert_df = create_alert_df(["MRN99999"], ["alerts_parent_baseline_1"], ["yes"])
    result = process_alerts_for_redcap(alert_df)
    assert len(result) == 0 or result["record"].isna().all()


# ============================================================================
# Tests - push_alerts_to_redcap
# ============================================================================


def test_push_alerts_calls_api(mock_alerts_dependencies, processed_alerts_for_push):
    """Test that push_alerts_to_redcap calls the API."""
    push_alerts_to_redcap(processed_alerts_for_push)
    push_mock = mock_alerts_dependencies["push"]
    assert push_mock.called
    assert len(push_mock.call_args[0][0]) == len(processed_alerts_for_push)


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
    mock_alerts_dependencies["push"].side_effect = Exception("API Error")
    with pytest.raises(Exception, match="API Error"):
        push_alerts_to_redcap(processed_alerts_for_push)


def test_push_alerts_kevin_urgent(mock_alerts_dependencies):
    """Test pushing Kevin's urgent alert."""
    kevin_alerts = create_alert_df(
        ["005", "005"],
        ["alerts_parent_baseline_5", "parent_baseline_alerts"],
        ["2", "yes"],
        ["baseline_arm_1", "baseline_arm_1"],
    )
    push_alerts_to_redcap(kevin_alerts)
    push_mock = mock_alerts_dependencies["push"]
    assert push_mock.called
    pushed_data = push_mock.call_args[0][0]
    assert len(pushed_data) == 2
    assert "2" in pushed_data["value"].values


# ============================================================================
# Tests - WebSocket Listener
# ============================================================================


@pytest.mark.asyncio
async def test_websocket_listener_processes_messages(
    mock_alerts_dependencies, sample_curious_alert, redcap_alerts_metadata
):
    """Test that websocket_listener processes messages correctly."""
    setup_standard_alert_mocks(mock_alerts_dependencies, redcap_alerts_metadata)

    mock_websocket = AsyncMock()
    mock_websocket.__aiter__.return_value = [json.dumps(sample_curious_alert)]

    with patch("hbnmigration.from_curious.alerts_to_redcap.parse_alert") as mock_parse:
        mock_parse.return_value = create_alert_df(
            ["MRN12345"], ["alerts_parent_baseline_1"], ["yes"]
        )
        await websocket_listener(mock_websocket, partial_redcap_landing=False)
        assert mock_parse.called
        assert mock_alerts_dependencies["push"].called


@pytest.mark.asyncio
async def test_websocket_listener_handles_connection_closed_error():
    """Test that websocket_listener raises ConnectionClosedError."""
    mock_websocket = AsyncMock()
    mock_websocket.__aiter__.side_effect = ConnectionClosedError(None, None)

    with pytest.raises(ConnectionClosedError):
        await websocket_listener(mock_websocket)


@pytest.mark.asyncio
async def test_websocket_listener_skips_non_answer_messages(mock_alerts_dependencies):
    """Test that non-answer messages are skipped."""
    mock_websocket = AsyncMock()
    non_answer = {"type": "heartbeat", "timestamp": "2024-01-01T00:00:00Z"}
    mock_websocket.__aiter__.return_value = [json.dumps(non_answer)]

    await websocket_listener(mock_websocket)
    assert not mock_alerts_dependencies["push"].called


# ============================================================================
# Tests - main_with_reconnect
# ============================================================================


@pytest.mark.asyncio
async def test_main_with_reconnect_successful_connection(
    mock_alerts_dependencies, redcap_alerts_metadata
):
    """Test that main_with_reconnect handles successful connection."""
    setup_standard_alert_mocks(mock_alerts_dependencies, redcap_alerts_metadata)

    with setup_reconnect_mocks() as mocks:
        await main_with_reconnect(
            token="test_token", uri="wss://test.com/alerts", max_attempts=1
        )

        assert mocks["ws"].called
        assert mocks["listener"].called


@pytest.mark.asyncio
async def test_main_with_reconnect_handles_connection_error(caplog):
    """Test that main_with_reconnect reconnects on ConnectionClosedError."""
    with setup_reconnect_mocks([ConnectionClosedError(None, None), None]) as mocks:
        await main_with_reconnect(
            token="test_token", uri="wss://test.com/alerts", max_attempts=2
        )

        assert mocks["listener"].call_count == 2
        assert "Reconnecting in" in caplog.text
        assert "Successfully reconnected" in caplog.text


@pytest.mark.asyncio
async def test_main_with_reconnect_max_attempts_exceeded():
    """Test that main_with_reconnect stops after max attempts."""
    with setup_reconnect_mocks(ConnectionClosedError(None, None)) as mocks:
        with pytest.raises(ConnectionClosedError):
            await main_with_reconnect(
                token="test_token", uri="wss://test.com/alerts", max_attempts=1
            )

        assert mocks["listener"].call_count == 1


@pytest.mark.asyncio
async def test_main_with_reconnect_handles_auth_error(caplog):
    """Test that main_with_reconnect handles 401 authentication errors."""
    with (
        patch(
            "hbnmigration.from_curious.alerts_to_redcap.connect_to_websocket"
        ) as mock_ws,
        patch("hbnmigration.from_curious.alerts_to_redcap.asyncio.sleep") as mock_sleep,
    ):
        mock_ws.side_effect = create_mock_invalid_status(401)
        mock_sleep.return_value = None

        with pytest.raises(InvalidStatus):
            await main_with_reconnect(
                token="test_token", uri="wss://test.com/alerts", max_attempts=1
            )

        assert "Authentication failed" in caplog.text


@pytest.mark.asyncio
async def test_main_with_reconnect_infinite_retries():
    """Test that main_with_reconnect runs indefinitely with None max_attempts."""
    side_effects = [ConnectionClosedError(None, None)] * 5 + [None]

    with setup_reconnect_mocks(side_effects) as mocks:
        await main_with_reconnect(
            token="test_token", uri="wss://test.com/alerts", max_attempts=None
        )

        assert mocks["listener"].call_count == 6
        assert mocks["sleep"].call_count == 5


# ============================================================================
# Tests - Integration (Async Main)
# ============================================================================


@pytest.mark.asyncio
async def test_main_processes_websocket_messages(
    mock_alerts_dependencies, sample_curious_alert, redcap_alerts_metadata
):
    """Test that main() processes websocket messages."""
    setup_standard_alert_mocks(mock_alerts_dependencies, redcap_alerts_metadata)

    with setup_main_test_mocks(
        mock_alerts_dependencies,
        sample_alert=sample_curious_alert,
        parse_return=create_alert_df(
            ["MRN12345"], ["alerts_parent_baseline_1"], ["yes"]
        ),
        metadata_return=redcap_alerts_metadata,
    ):
        with patch(
            "hbnmigration.from_curious.alerts_to_redcap.main_with_reconnect"
        ) as mock_reconnect:
            mock_reconnect.return_value = None
            await main()
            assert mock_reconnect.called
            call_kwargs = mock_reconnect.call_args[1]
            assert call_kwargs["token"] == "test_token"
            assert "wss://" in call_kwargs["uri"]


@pytest.mark.asyncio
async def test_main_passes_max_attempts():
    """Test that main() passes max_attempts parameter."""
    with (
        patch(
            "hbnmigration.from_curious.alerts_to_redcap.curious_authenticate"
        ) as mock_auth,
        patch(
            "hbnmigration.from_curious.alerts_to_redcap.main_with_reconnect"
        ) as mock_reconnect,
    ):
        mock_auth.return_value = create_mock_tokens_ws()
        mock_reconnect.return_value = None

        await main(partial_redcap_landing=False, max_attempts=10)

        call_kwargs = mock_reconnect.call_args[1]
        assert call_kwargs["max_attempts"] == 10


@pytest.mark.asyncio
async def test_main_skips_non_answer_messages(mock_alerts_dependencies):
    """Test that main() skips non-answer message types."""
    non_answer = {"type": "heartbeat", "timestamp": "2024-01-01T00:00:00Z"}
    with setup_main_test_mocks(mock_alerts_dependencies, sample_alert=non_answer):
        with patch(
            "hbnmigration.from_curious.alerts_to_redcap.main_with_reconnect"
        ) as mock_reconnect:
            mock_reconnect.return_value = None
            await main()
            assert not mock_alerts_dependencies["push"].called


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
    with setup_sync_main_mocks(
        mock_alerts_dependencies,
        multiple_curious_alerts,
        parse_returns,
        metadata_return=redcap_alerts_metadata,
    ) as test_mocks:
        synchronous_main()
        assert test_mocks["parse"].call_count == 3


def test_synchronous_main_handles_api_error(mock_alerts_dependencies):
    """Test that synchronous_main() handles API errors gracefully."""
    with setup_sync_main_mocks(mock_alerts_dependencies, [], None, status_code=500):
        synchronous_main()
        assert not mock_alerts_dependencies["push"].called


def test_synchronous_main_partial_mode(
    mock_alerts_dependencies, redcap_alerts_metadata
):
    """Test synchronous_main with partial_redcap_landing flag."""
    setup_standard_alert_mocks(mock_alerts_dependencies, redcap_alerts_metadata)

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
    with setup_sync_main_mocks(
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
    with setup_cli_mocks() as mocks:
        cli()
        assert mocks["run"].called
        assert mocks["run"].call_count == 1


@pytest.mark.filterwarnings("ignore:coroutine 'main' was never awaited:RuntimeWarning")
def test_cli_sync_mode():
    """Test CLI runs in sync mode with --synchronous flag."""
    with setup_cli_mocks(argv=["alerts_to_redcap.py", "--synchronous"]) as mocks:
        cli()
        assert mocks["sync"].called
        assert not mocks["run"].called


@pytest.mark.filterwarnings("ignore:coroutine 'main' was never awaited:RuntimeWarning")
def test_cli_partial_mode():
    """Test CLI passes partial flag correctly."""
    with setup_cli_mocks(
        argv=["alerts_to_redcap.py", "--synchronous", "--partial"]
    ) as mocks:
        cli()
        assert mocks["sync"].called
        assert mocks["sync"].call_args[0][0] is True
        assert not mocks["run"].called


def test_cli_max_reconnect_attempts():
    """Test CLI passes max_reconnect_attempts flag correctly."""
    with setup_cli_mocks(
        argv=["alerts_to_redcap.py", "--max-reconnect-attempts", "5"]
    ) as mocks:
        cli()
        assert mocks["run"].called


# ============================================================================
# Tests - response_index_reverse_lookup
# ============================================================================


@pytest.mark.parametrize(
    "field_name,choices,expected",
    [
        (
            "test_field",
            "0, No | 1, Yes | 2, Maybe",
            [
                ("test_field", "no", 0),
                ("test_field", "yes", 1),
                ("test_field", "maybe", 2),
            ],
        ),
        ("text_field", "", []),
        ("test_field", "invalid_format", []),
        (
            "alerts_parent_baseline_5",
            "0, Normal | 1, Concerning | 2, Urgent",
            [
                ("alerts_parent_baseline_5", "normal", 0),
                ("alerts_parent_baseline_5", "concerning", 1),
                ("alerts_parent_baseline_5", "urgent", 2),
            ],
        ),
    ],
)
def test_response_index_reverse_lookup(field_name, choices, expected):
    """Test response_index_reverse_lookup with various inputs - now returns a list."""
    row = pd.Series(
        {
            "field_name": field_name,
            "select_choices_or_calculations": choices,
        }
    )
    result = response_index_reverse_lookup(row)
    assert result == expected
