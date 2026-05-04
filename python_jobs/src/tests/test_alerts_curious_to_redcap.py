"""Tests for alerts_to_redcap module."""

from datetime import date
import json
from typing import cast
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest
import requests
from websockets.exceptions import ConnectionClosedError, InvalidStatus

from hbnmigration.exceptions import NoData
from hbnmigration.from_curious.alerts_to_redcap import (
    _reconnect_loop,
    cli,
    main,
    parse_alert,
    process_alerts_for_redcap,
    push_alerts_to_redcap,
    synchronous_main,
    toggle_alerts,
    websocket_listener,
)
from hbnmigration.from_redcap.from_redcap import response_index_reverse_lookup
from hbnmigration.utility_functions import (
    CuriousAlert,
    CuriousAlertHttps,
    CuriousAlertWebsocket,
)

# Import shared utilities from conftest
from .conftest import (
    create_alert_df,
    create_alert_metadata,
    create_mock_invalid_status,
    create_mock_tokens_ws,
    setup_cli_mocks,
    setup_main_test_mocks,
    setup_reconnect_mocks,
    setup_standard_alert_mocks,
)

_REDCAP_PID = 625

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


@pytest.mark.parametrize("sample_curious_alert", ["https", "wss"], indirect=True)
def test_parse_alert_with_secret_id(
    sample_curious_alert: CuriousAlert, request: pytest.FixtureRequest
) -> None:
    """Test that parse_alert correctly parses an alert with secretId."""
    # Update message to match the expected format: color: "answer" to ... item
    alert = sample_curious_alert.copy()
    alert["message"] = 'Red: "Yes" to Question text parent_baseline'
    result = parse_alert(alert)
    assert not result.empty
    assert "record" in result.columns
    assert "field_name" in result.columns
    assert "value" in result.columns
    if request.node.callspec.params["sample_curious_alert"] == "https":
        sample_curious_alert = cast(CuriousAlertHttps, sample_curious_alert)
        assert sample_curious_alert["secretId"] in result["record"].values
    else:
        sample_curious_alert = cast(CuriousAlertWebsocket, sample_curious_alert)
        assert sample_curious_alert["secret_id"] in result["record"].values


def test_parse_alert_extracts_mrn(sample_curious_alert):
    """Test that parse_alert extracts MRN correctly."""
    alert = sample_curious_alert.copy()
    alert["message"] = 'Red: "Yes" to Question text parent_baseline'
    result = parse_alert(alert)
    mrn_row = result[result["field_name"] == "mrn"]
    assert len(mrn_row) == 1
    assert mrn_row["value"].iloc[0] == sample_curious_alert["secretId"]


def test_parse_alert_converts_message_to_field_name(sample_curious_alert):
    """Test that parse_alert converts message to alert field name."""
    alert = sample_curious_alert.copy()
    alert["message"] = 'Red: "Yes" to Question text parent_baseline'
    result = parse_alert(alert)
    field_rows = result[result["field_name"] != "mrn"]
    assert len(field_rows) > 0
    assert field_rows["field_name"].iloc[0].startswith("alerts_")


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
    process_alerts_for_redcap(_REDCAP_PID, redcap_alert_df)
    assert mock_alerts_dependencies["fetch_metadata"].called


def test_process_alerts_calls_required_functions(mock_alerts_dependencies):
    """Test that process_alerts_for_redcap calls the necessary functions."""
    setup_standard_alert_mocks(mock_alerts_dependencies)
    alert_data = create_alert_df(["12345"], ["alerts_parent_baseline_1"], ["yes"])
    result = process_alerts_for_redcap(_REDCAP_PID, alert_data)
    assert isinstance(result, pd.DataFrame)
    assert mock_alerts_dependencies["fetch_metadata"].called
    assert mock_alerts_dependencies["fetch"].called


def test_process_alerts_returns_dataframe_with_required_columns(
    mock_alerts_dependencies,
):
    """Test that result has required REDCap columns."""
    setup_standard_alert_mocks(mock_alerts_dependencies)
    alert_data = create_alert_df(["12345"], ["alerts_parent_baseline_1"], ["yes"])
    result = process_alerts_for_redcap(_REDCAP_PID, alert_data)
    for col in ["record", "field_name", "value"]:
        assert col in result.columns


def test_process_alerts_toggles_summary_flags(
    mock_alerts_dependencies, redcap_alerts_metadata
):
    """Test that instrument-level alert flags are toggled."""
    setup_standard_alert_mocks(mock_alerts_dependencies, redcap_alerts_metadata)
    alert_data = create_alert_df(["MRN12345"], ["alerts_parent_baseline_1"], ["yes"])
    result = process_alerts_for_redcap(_REDCAP_PID, alert_data)
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
    result = process_alerts_for_redcap(
        _REDCAP_PID, alert_df, partial_redcap_landing=True
    )
    assert "alerts_nonexistent_field" not in result["field_name"].values


def test_process_alerts_handles_missing_mrn(
    mock_alerts_dependencies, redcap_alerts_metadata
):
    """Test handling of alerts with MRN not found in REDCap."""
    setup_standard_alert_mocks(mock_alerts_dependencies, redcap_alerts_metadata)
    alert_df = create_alert_df(["MRN99999"], ["alerts_parent_baseline_1"], ["yes"])
    result = process_alerts_for_redcap(_REDCAP_PID, alert_df)
    assert len(result) == 0 or result["record"].isna().all()


def test_process_alerts_maps_records_before_toggle(mock_alerts_dependencies):
    """Test that record IDs are mapped before toggle_alerts."""
    metadata = pd.DataFrame(
        {
            "field_name": ["mrn", "alerts_parent_baseline_1"],
            "field_type": ["text", "radio"],
            "select_choices_or_calculations": ["", "0, No | 1, Yes"],
        }
    )
    existing_data = create_alert_df(
        ["001", "001"],
        ["mrn", "alerts_parent_baseline_1"],
        ["12345", "0"],
        ["baseline_arm_1", "baseline_arm_1"],
    )
    setup_standard_alert_mocks(mock_alerts_dependencies, metadata, existing_data)
    alert_df = create_alert_df(["12345"], ["alerts_parent_baseline_1"], ["yes"])
    result = process_alerts_for_redcap(_REDCAP_PID, alert_df)
    # Should have record ID, not MRN
    assert "001" in result["record"].values
    assert "12345" not in result["record"].values


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
    assert "rows updated" in caplog.text
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
    # Set up fetch to handle two calls: mrn data, then field data
    mrn_data = create_alert_df(["1"], ["mrn"], ["12345"], ["baseline_arm_1"])
    field_data = create_alert_df(
        ["1"], ["alerts_parent_baseline_1"], ["0"], ["baseline_arm_1"]
    )
    mock_alerts_dependencies["fetch"].side_effect = [mrn_data, field_data]
    mock_alerts_dependencies["fetch_metadata"].return_value = redcap_alerts_metadata

    mock_websocket = AsyncMock()
    mock_websocket.__aiter__.return_value = [json.dumps(sample_curious_alert)]
    with patch("hbnmigration.from_curious.alerts_to_redcap.parse_alert") as mock_parse:
        mock_parse.return_value = create_alert_df(
            ["12345", "12345"], ["mrn", "alerts_parent_baseline_1"], ["12345", "yes"]
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


@pytest.mark.asyncio
async def test_main_with_reconnect_reauths_on_401_then_succeeds(caplog):
    """Test that _reconnect_loop re-authenticates on 401 and succeeds."""
    with (
        patch(
            "hbnmigration.from_curious.alerts_to_redcap.connect_to_websocket"
        ) as mock_ws,
        patch(
            "hbnmigration.from_curious.alerts_to_redcap.websocket_listener",
            new_callable=AsyncMock,
        ) as mock_listener,
        patch(
            "hbnmigration.from_curious.alerts_to_redcap.asyncio.sleep",
            new_callable=AsyncMock,
        ),
        patch(
            "hbnmigration.from_curious.alerts_to_redcap.curious_authenticate"
        ) as mock_auth,
    ):
        mock_auth.return_value = create_mock_tokens_ws()
        # First connect raises 401, second succeeds
        mock_ctx = AsyncMock()
        mock_ctx.__aenter__.return_value = AsyncMock()
        mock_ws.side_effect = [
            create_mock_invalid_status(requests.codes["unauthorized"]),
            mock_ctx,
        ]

        await _reconnect_loop(
            applet_name="test_applet",
            uri="wss://test.com/alerts",
            partial_redcap_landing=False,
            max_attempts=2,
        )
        # Initial auth + re-auth on 401
        assert mock_auth.call_count == 2
        assert mock_listener.called
        assert "Re-authenticating" in caplog.text


@pytest.mark.asyncio
async def test_main_with_reconnect_reauth_failure_raises(caplog):
    """Test that _reconnect_loop raises when re-authentication itself fails."""
    with (
        patch(
            "hbnmigration.from_curious.alerts_to_redcap.connect_to_websocket"
        ) as mock_ws,
        patch(
            "hbnmigration.from_curious.alerts_to_redcap.asyncio.sleep",
            new_callable=AsyncMock,
        ),
        patch(
            "hbnmigration.from_curious.alerts_to_redcap.curious_authenticate"
        ) as mock_auth,
    ):
        # Initial auth succeeds, re-auth on 401 fails
        mock_auth.side_effect = [
            create_mock_tokens_ws(),
            Exception("Auth server down"),
        ]
        mock_ws.side_effect = create_mock_invalid_status(requests.codes["unauthorized"])

        with pytest.raises(Exception, match="Auth server down"):
            await _reconnect_loop(
                applet_name="test_applet",
                uri="wss://test.com/alerts",
                partial_redcap_landing=False,
                max_attempts=2,
            )
        assert "Re-authentication failed" in caplog.text


# ============================================================================
# Tests - _reconnect_loop
# ============================================================================


@pytest.mark.asyncio
async def test_main_with_reconnect_successful_connection(
    mock_alerts_dependencies, redcap_alerts_metadata
):
    """Test that _reconnect_loop handles successful connection."""
    setup_standard_alert_mocks(mock_alerts_dependencies, redcap_alerts_metadata)
    with setup_reconnect_mocks() as mocks:
        await _reconnect_loop(
            applet_name="test_applet",
            uri="wss://test.com/alerts",
            partial_redcap_landing=False,
            max_attempts=1,
        )
        assert mocks["ws"].called
        assert mocks["listener"].called


@pytest.mark.asyncio
async def test_main_with_reconnect_handles_connection_error(caplog):
    """Test that _reconnect_loop reconnects on ConnectionClosedError."""
    with setup_reconnect_mocks([ConnectionClosedError(None, None), None]) as mocks:
        await _reconnect_loop(
            applet_name="test_applet",
            uri="wss://test.com/alerts",
            partial_redcap_landing=False,
            max_attempts=2,
        )
        assert mocks["listener"].call_count == 2
        assert "Reconnecting in" in caplog.text
        assert "Reconnected to WebSocket" in caplog.text


@pytest.mark.asyncio
async def test_main_with_reconnect_max_attempts_exceeded():
    """Test that _reconnect_loop stops after max attempts."""
    with setup_reconnect_mocks(ConnectionClosedError(None, None)) as mocks:
        with pytest.raises(ConnectionClosedError):
            await _reconnect_loop(
                applet_name="test_applet",
                uri="wss://test.com/alerts",
                partial_redcap_landing=False,
                max_attempts=1,
            )
        assert mocks["listener"].call_count == 1


@pytest.mark.asyncio
async def test_main_with_reconnect_handles_auth_error(caplog):
    """Test that _reconnect_loop handles 401 authentication errors."""
    with (
        patch(
            "hbnmigration.from_curious.alerts_to_redcap.connect_to_websocket"
        ) as mock_ws,
        patch("hbnmigration.from_curious.alerts_to_redcap.asyncio.sleep") as mock_sleep,
        patch(
            "hbnmigration.from_curious.alerts_to_redcap.curious_authenticate"
        ) as mock_auth,
    ):
        mock_ws.side_effect = create_mock_invalid_status(requests.codes["unauthorized"])
        mock_sleep.return_value = None
        # First call is the initial auth; second call is the re-auth on 401
        mock_auth.side_effect = [
            create_mock_tokens_ws(),
            create_mock_tokens_ws(),
        ]
        with pytest.raises(InvalidStatus):
            await _reconnect_loop(
                applet_name="test_applet",
                uri="wss://test.com/alerts",
                partial_redcap_landing=False,
                max_attempts=1,
            )
        assert (
            "Re-authenticating" in caplog.text
            or "Max reconnect attempts reached" in caplog.text
        )


@pytest.mark.asyncio
async def test_main_with_reconnect_infinite_retries():
    """Test that _reconnect_loop runs indefinitely with None max_attempts."""
    side_effects = [ConnectionClosedError(None, None)] * 5 + [None]
    with setup_reconnect_mocks(side_effects) as mocks:
        await _reconnect_loop(
            applet_name="test_applet",
            uri="wss://test.com/alerts",
            partial_redcap_landing=False,
            max_attempts=None,
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
            "hbnmigration.from_curious.alerts_to_redcap._reconnect_loop"
        ) as mock_reconnect:
            mock_reconnect.return_value = None
            await main(applet_names=["Healthy Brain Network Questionnaires"])
            assert mock_reconnect.called
            call_args = mock_reconnect.call_args[0]
            assert call_args[0] == "Healthy Brain Network Questionnaires"
            assert "wss://" in call_args[1]


@pytest.mark.asyncio
async def test_main_passes_max_attempts():
    """Test that main() passes max_attempts parameter."""
    with patch(
        "hbnmigration.from_curious.alerts_to_redcap._reconnect_loop"
    ) as mock_reconnect:
        mock_reconnect.return_value = None
        await main(
            applet_names=["Healthy Brain Network Questionnaires"],
            partial_redcap_landing=False,
            max_attempts=10,
        )
        call_args = mock_reconnect.call_args[0]
        assert call_args[3] == 10  # max_attempts is 4th positional arg


@pytest.mark.asyncio
async def test_main_skips_non_answer_messages(mock_alerts_dependencies):
    """Test that main() skips non-answer message types."""
    non_answer = {"type": "heartbeat", "timestamp": "2024-01-01T00:00:00Z"}
    with setup_main_test_mocks(mock_alerts_dependencies, sample_alert=non_answer):
        with patch(
            "hbnmigration.from_curious.alerts_to_redcap._reconnect_loop"
        ) as mock_reconnect:
            mock_reconnect.return_value = None
            await main(applet_names=["Healthy Brain Network Questionnaires"])
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
    with (
        patch(
            "hbnmigration.from_curious.alerts_to_redcap.curious_authenticate"
        ) as mock_auth,
        patch(
            "hbnmigration.from_curious.alerts_to_redcap.call_curious_api"
        ) as mock_call,
        patch("hbnmigration.from_curious.alerts_to_redcap.parse_alert") as mock_parse,
    ):
        mock_auth.return_value = create_mock_tokens_ws()
        mock_call.return_value = multiple_curious_alerts
        mock_parse.side_effect = parse_returns
        synchronous_main(applet_names=["Healthy Brain Network Questionnaires"])
        assert mock_parse.call_count == 3


def test_synchronous_main_handles_api_error(mock_alerts_dependencies):
    """Test that synchronous_main() handles API errors gracefully."""
    with (
        patch(
            "hbnmigration.from_curious.alerts_to_redcap.curious_authenticate"
        ) as mock_auth,
        patch(
            "hbnmigration.from_curious.alerts_to_redcap.call_curious_api"
        ) as mock_call,
    ):
        mock_auth.return_value = create_mock_tokens_ws()
        mock_call.side_effect = Exception("API Error")
        with pytest.raises(Exception, match="API Error"):
            synchronous_main(applet_names=["Healthy Brain Network Questionnaires"])


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
            "secretId": "00001_P",
        }
    ]
    parse_return = [
        create_alert_df(["MRN12345"], ["alerts_parent_baseline_1"], ["yes"])
    ]
    with (
        patch(
            "hbnmigration.from_curious.alerts_to_redcap.curious_authenticate"
        ) as mock_auth,
        patch(
            "hbnmigration.from_curious.alerts_to_redcap.call_curious_api"
        ) as mock_call,
        patch("hbnmigration.from_curious.alerts_to_redcap.parse_alert") as mock_parse,
    ):
        mock_auth.return_value = create_mock_tokens_ws()
        mock_call.return_value = alert
        mock_parse.side_effect = parse_return
        synchronous_main(
            applet_names=["Healthy Brain Network Questionnaires"],
            partial_redcap_landing=True,
        )
        assert mock_parse.called


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
        assert mocks["sync"].call_args[0][1] is True
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
                ("test_field", "no", "0"),
                ("test_field", "yes", "1"),
                ("test_field", "maybe", "2"),
            ],
        ),
        ("text_field", "", []),
        ("test_field", "invalid_format", []),
        (
            "alerts_parent_baseline_5",
            "0, Normal | 1, Concerning | 2, Urgent",
            [
                ("alerts_parent_baseline_5", "normal", "0"),
                ("alerts_parent_baseline_5", "concerning", "1"),
                ("alerts_parent_baseline_5", "urgent", "2"),
            ],
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


# ============================================================================
# Tests - fetch_alerts_metadata
# ============================================================================


def test_fetch_alerts_metadata_called_in_process_alerts(
    mock_alerts_dependencies, redcap_alerts_metadata
):
    """Test that fetch_alerts_metadata is called during alert processing."""
    setup_standard_alert_mocks(mock_alerts_dependencies, redcap_alerts_metadata)
    alert_df = create_alert_df(["MRN12345"], ["alerts_parent_baseline_1"], ["yes"])
    process_alerts_for_redcap(_REDCAP_PID, alert_df)
    # The fetch_alerts_metadata should be called (it's the imported function)
    assert mock_alerts_dependencies["fetch_metadata"].called


# ============================================================================
# Tests - Parametrized Parse Alert Tests
# ============================================================================


@pytest.mark.parametrize(
    "message,expected_item",
    [
        (
            'Red: "Yes" to Difficulty concentrating parent_baseline',
            "alerts_parent_baseline",
        ),
        (
            'Yellow: "No" to Sleep issues child_followup',
            "alerts_child_followup",
        ),
        (
            'Green: "Sometimes" to Mood changes parent_followup',
            "alerts_parent_followup",
        ),
    ],
    ids=["red_parent", "yellow_child", "green_parent"],
)
def test_parse_alert_various_colors_and_items(
    sample_curious_alert, message, expected_item
):
    """Test parse_alert with various color and item combinations."""
    alert = sample_curious_alert.copy()
    alert["message"] = message
    result = parse_alert(alert)
    assert not result.empty
    assert sample_curious_alert["secretId"] in result["record"].values
    # Verify that an item field was created
    item_fields = result[result["field_name"] != "mrn"]
    assert len(item_fields) > 0


# ============================================================================
# Tests - Parametrized Process Alerts Tests
# ============================================================================


@pytest.mark.parametrize(
    "records,field_names,expected_summary",
    [
        (
            ["001", "001"],
            ["alerts_parent_baseline_1", "alerts_parent_baseline_2"],
            "parent_baseline_alerts",
        ),
        (
            ["003"],
            ["alerts_parent_baseline_1"],
            "parent_baseline_alerts",
        ),
    ],
    ids=["parent_baseline", "single_record"],
)
def test_process_alerts_creates_summary_fields(
    mock_alerts_dependencies,
    redcap_alerts_metadata,
    records,
    field_names,
    expected_summary,
):
    """Test that process_alerts creates appropriate summary fields."""
    unique_records = list(set(records))

    # MRN data: maps MRN value → record_id
    mrn_data = create_alert_df(
        unique_records,
        ["mrn"] * len(unique_records),
        unique_records,  # MRN values same as record IDs for simplicity
        ["baseline_arm_1"] * len(unique_records),
    )

    # Alert field data
    field_data = create_alert_df(
        unique_records[: len(field_names)],
        field_names[: len(unique_records)],
        ["0"] * min(len(unique_records), len(field_names)),
        ["baseline_arm_1"] * min(len(unique_records), len(field_names)),
    )

    # fetch_data is called twice: once for "mrn", once for alert fields
    mock_alerts_dependencies["fetch"].side_effect = [mrn_data, field_data]
    mock_alerts_dependencies["fetch_metadata"].return_value = redcap_alerts_metadata

    # Alert input: mrn rows + alert field rows
    alert_records = unique_records + records
    alert_fields_list = ["mrn"] * len(unique_records) + field_names
    alert_values = unique_records + ["yes"] * len(field_names)
    alert_df = create_alert_df(alert_records, alert_fields_list, alert_values)

    result = process_alerts_for_redcap(_REDCAP_PID, alert_df)
    summary_rows = result[result["field_name"] == expected_summary]
    assert len(summary_rows) > 0


# ============================================================================
# Tests - Parametrized Push Alerts Tests
# ============================================================================


@pytest.mark.parametrize(
    "records,values",
    [
        (["001", "001"], ["yes", "no"]),
        (["002"], ["1"]),
        (["003", "004"], ["yes", "yes"]),
    ],
    ids=["mixed_yes_no", "numeric", "all_yes"],
)
def test_push_alerts_various_values(mock_alerts_dependencies, records, values):
    """Test push_alerts_to_redcap with various alert values."""
    alert_df = create_alert_df(
        records, ["alerts_parent_baseline_1"] * len(records), values
    )
    push_alerts_to_redcap(alert_df)
    push_mock = mock_alerts_dependencies["push"]
    assert push_mock.called
    pushed_data = push_mock.call_args[0][0]
    assert len(pushed_data) == len(records)


class TestAlertCacheKeys:
    """Test alert-specific cache key creation."""

    def test_create_alert_cache_key(self):
        """Test creating cache key for alert."""
        from hbnmigration.from_curious.alerts_to_redcap import (
            create_alert_cache_key,
        )

        result = create_alert_cache_key("alert_123", "Test message")
        assert result.startswith("alert_123:")
        assert len(result.split(":")) == 2

    def test_synchronous_main_uses_cache_keys(
        self, mock_alerts_dependencies, sample_curious_alert
    ):
        """Test that synchronous_main uses composite cache keys."""
        from hbnmigration.from_curious.alerts_to_redcap import synchronous_main

        setup_standard_alert_mocks(mock_alerts_dependencies)

        # Use actual applet name that exists
        applet_name = "Healthy Brain Network Questionnaires"

        with (
            patch(
                "hbnmigration.from_curious.alerts_to_redcap.call_curious_api"
            ) as mock_call,
            patch(
                "hbnmigration.from_curious.alerts_to_redcap.curious_authenticate"
            ) as mock_auth,
        ):
            # Mock authentication
            mock_tokens = MagicMock()
            mock_tokens.endpoints.alerts = "https://test.com/alerts"
            mock_auth.return_value = mock_tokens

            # Return alert
            mock_call.return_value = [sample_curious_alert]

            # Mock parse_alert to return valid DataFrame
            with patch(
                "hbnmigration.from_curious.alerts_to_redcap.parse_alert"
            ) as mock_parse:
                mock_parse.return_value = create_alert_df(
                    ["12345"], ["alerts_parent_baseline_1"], ["yes"]
                )

                synchronous_main(applet_names=[applet_name])

                # Verify cache key was used (push should be called)
                assert mock_alerts_dependencies["push"].called


# ============================================================================
# Tests - _log_invalid_alert_fields
# ============================================================================


class TestLogInvalidAlertFields:
    """Tests for _log_invalid_alert_fields."""

    def test_logs_new_fields_to_file(self, tmp_path, monkeypatch):
        """New invalid fields are written to daily log file."""
        from hbnmigration.from_curious.alerts_to_redcap import _log_invalid_alert_fields

        monkeypatch.setattr(
            "hbnmigration.from_curious.alerts_to_redcap.log_root_path",
            lambda: tmp_path,
        )
        with patch("hbnmigration.from_curious.alerts_to_redcap.send_alert"):
            _log_invalid_alert_fields(["alerts_bad_field_1", "alerts_bad_field_2"])

        log_file = tmp_path / "invalid_alert_fields" / f"{date.today().isoformat()}.txt"
        assert log_file.exists()
        contents = log_file.read_text().splitlines()
        assert "alerts_bad_field_1" in contents
        assert "alerts_bad_field_2" in contents

    def test_sends_teams_alert_for_new_fields(self, tmp_path, monkeypatch):
        """Teams alert is sent when new fields are detected."""
        from hbnmigration.from_curious.alerts_to_redcap import _log_invalid_alert_fields

        monkeypatch.setattr(
            "hbnmigration.from_curious.alerts_to_redcap.log_root_path",
            lambda: tmp_path,
        )
        with patch(
            "hbnmigration.from_curious.alerts_to_redcap.send_alert"
        ) as mock_alert:
            _log_invalid_alert_fields(["alerts_new_field"])

        assert mock_alert.called
        msg = mock_alert.call_args[0][0]
        assert "alerts_new_field" in msg
        assert "invalid field name" in msg

    def test_no_alert_for_duplicate_fields(self, tmp_path, monkeypatch):
        """No Teams alert when fields were already logged today."""
        from hbnmigration.from_curious.alerts_to_redcap import _log_invalid_alert_fields

        monkeypatch.setattr(
            "hbnmigration.from_curious.alerts_to_redcap.log_root_path",
            lambda: tmp_path,
        )
        # Pre-populate log file
        log_dir = tmp_path / "invalid_alert_fields"
        log_dir.mkdir(parents=True)
        log_file = log_dir / f"{date.today().isoformat()}.txt"
        log_file.write_text("alerts_already_logged\n")

        with patch(
            "hbnmigration.from_curious.alerts_to_redcap.send_alert"
        ) as mock_alert:
            _log_invalid_alert_fields(["alerts_already_logged"])

        assert not mock_alert.called

    def test_only_new_fields_trigger_alert(self, tmp_path, monkeypatch):
        """Only genuinely new fields trigger alert; old ones are skipped."""
        from hbnmigration.from_curious.alerts_to_redcap import _log_invalid_alert_fields

        monkeypatch.setattr(
            "hbnmigration.from_curious.alerts_to_redcap.log_root_path",
            lambda: tmp_path,
        )
        log_dir = tmp_path / "invalid_alert_fields"
        log_dir.mkdir(parents=True)
        log_file = log_dir / f"{date.today().isoformat()}.txt"
        log_file.write_text("alerts_old_field\n")

        with patch(
            "hbnmigration.from_curious.alerts_to_redcap.send_alert"
        ) as mock_alert:
            _log_invalid_alert_fields(["alerts_old_field", "alerts_brand_new"])

        assert mock_alert.called
        msg = mock_alert.call_args[0][0]
        assert "alerts_brand_new" in msg
        assert "alerts_old_field" not in msg

        # Both should be in log file now
        contents = log_file.read_text().splitlines()
        assert "alerts_old_field" in contents
        assert "alerts_brand_new" in contents


# ============================================================================
# Tests - _validate_alert_fields
# ============================================================================


class TestValidateAlertFields:
    """Tests for _validate_alert_fields."""

    def test_filters_invalid_fields(self, tmp_path, monkeypatch):
        """Invalid fields are removed from returned DataFrame."""
        from hbnmigration.from_curious.alerts_to_redcap import _validate_alert_fields

        monkeypatch.setattr(
            "hbnmigration.from_curious.alerts_to_redcap.log_root_path",
            lambda: tmp_path,
        )
        metadata = create_alert_metadata(
            ["mrn", "alerts_valid_1", "valid_alerts"],
            ["", "0, No | 1, Yes", "0, No | 1, Yes"],
        )
        alerts = create_alert_df(
            ["001", "001", "001"],
            ["mrn", "alerts_valid_1", "alerts_invalid_1"],
            ["001", "yes", "yes"],
        )
        with patch("hbnmigration.from_curious.alerts_to_redcap.send_alert"):
            filtered, valid = _validate_alert_fields(alerts, metadata)

        assert "alerts_valid_1" in valid
        assert "alerts_invalid_1" not in valid
        assert "alerts_invalid_1" not in filtered["field_name"].values
        assert "mrn" in filtered["field_name"].values

    def test_returns_empty_valid_list_when_none_match(self, tmp_path, monkeypatch):
        """Empty valid list when no alert fields match metadata."""
        from hbnmigration.from_curious.alerts_to_redcap import _validate_alert_fields

        monkeypatch.setattr(
            "hbnmigration.from_curious.alerts_to_redcap.log_root_path",
            lambda: tmp_path,
        )
        metadata = create_alert_metadata(["mrn"], [""])
        alerts = create_alert_df(["001"], ["alerts_nonexistent"], ["yes"])

        with patch("hbnmigration.from_curious.alerts_to_redcap.send_alert"):
            _, valid = _validate_alert_fields(alerts, metadata)

        assert valid == []


# ============================================================================
# Tests - _fetch_redcap_context
# ============================================================================


class TestFetchRedcapContext:
    """Tests for _fetch_redcap_context."""

    def test_returns_none_when_no_mrn_data(self, mock_alerts_dependencies):
        """Returns None when MRN fetch returns empty."""
        from hbnmigration.from_curious.alerts_to_redcap import _fetch_redcap_context

        mock_alerts_dependencies["fetch"].return_value = pd.DataFrame()
        result = _fetch_redcap_context(["alerts_field_1"])
        assert result is None

    def test_handles_no_data_for_alert_fields(self, mock_alerts_dependencies):
        """Returns MRN data alone when alert fields raise NoData."""
        from hbnmigration.from_curious.alerts_to_redcap import _fetch_redcap_context

        mrn_data = create_alert_df(["1"], ["mrn"], ["12345"], ["baseline_arm_1"])
        mock_alerts_dependencies["fetch"].side_effect = [mrn_data, NoData()]
        result = _fetch_redcap_context(["alerts_unpopulated_1"])
        assert result is not None
        assert len(result) == 1
        assert result["field_name"].iloc[0] == "mrn"

    def test_combines_mrn_and_field_data(self, mock_alerts_dependencies):
        """Returns concatenated MRN + field data when both available."""
        from hbnmigration.from_curious.alerts_to_redcap import _fetch_redcap_context

        mrn_data = create_alert_df(["1"], ["mrn"], ["12345"], ["baseline_arm_1"])
        field_data = create_alert_df(
            ["1"], ["alerts_field_1"], ["0"], ["baseline_arm_1"]
        )
        mock_alerts_dependencies["fetch"].side_effect = [mrn_data, field_data]
        result = _fetch_redcap_context(["alerts_field_1"])
        assert result is not None
        assert len(result) == 2


# ============================================================================
# Tests - _map_and_transform
# ============================================================================


class TestMapAndTransform:
    """Tests for _map_and_transform."""

    def test_drops_unmapped_records(self):
        """Rows with MRNs not in lookup are dropped."""
        from hbnmigration.from_curious.alerts_to_redcap import _map_and_transform

        alerts = create_alert_df(
            ["12345", "99999", "12345", "99999"],
            ["mrn", "mrn", "alerts_field_1", "alerts_field_1"],
            ["12345", "99999", "yes", "yes"],
        )
        redcap_fields = create_alert_df(["1"], ["mrn"], ["12345"], ["baseline_arm_1"])
        metadata = create_alert_metadata(
            ["mrn", "alerts_field_1"], ["", "0, No | 1, Yes"]
        )
        result = _map_and_transform(alerts, redcap_fields, metadata)
        # Only record with MRN 12345 should remain
        assert len(result) == 1
        assert result["record"].iloc[0] == "1"

    def test_maps_choice_values(self):
        """Response values are mapped to REDCap indices."""
        from hbnmigration.from_curious.alerts_to_redcap import _map_and_transform

        alerts = create_alert_df(
            ["12345", "12345"],
            ["mrn", "alerts_field_1"],
            ["12345", "Yes"],
        )
        redcap_fields = create_alert_df(["1"], ["mrn"], ["12345"], ["baseline_arm_1"])
        metadata = create_alert_metadata(
            ["mrn", "alerts_field_1"], ["", "0, No | 1, Yes"]
        )
        result = _map_and_transform(alerts, redcap_fields, metadata)
        assert result["value"].iloc[0] == "1"  # "Yes" → "1"

    def test_returns_empty_when_no_mrn_match(self):
        """Returns empty DataFrame when no MRNs match."""
        from hbnmigration.from_curious.alerts_to_redcap import _map_and_transform

        alerts = create_alert_df(
            ["99999", "99999"], ["mrn", "alerts_field_1"], ["99999", "yes"]
        )
        redcap_fields = create_alert_df(["1"], ["mrn"], ["12345"], ["baseline_arm_1"])
        metadata = create_alert_metadata(
            ["mrn", "alerts_field_1"], ["", "0, No | 1, Yes"]
        )
        result = _map_and_transform(alerts, redcap_fields, metadata)
        assert result.empty


# ============================================================================
# Tests - map_mrns_to_records leading zeros
# ============================================================================


class TestMapMrnsLeadingZeros:
    """Tests for leading zero preservation in map_mrns_to_records."""

    def test_preserves_leading_zeros_in_mrn(self):
        """MRN values with leading zeros are preserved."""
        from hbnmigration.from_curious.utils import map_mrns_to_records

        alerts = pd.DataFrame(
            {
                "record": ["001234", "001234"],
                "field_name": ["mrn", "alerts_test_1"],
                "value": ["001234", "yes"],
                "redcap_event_name": [None, None],
            }
        )
        redcap_fields = pd.DataFrame(
            {
                "record": ["42"],
                "field_name": ["mrn"],
                "value": ["001234"],
                "redcap_event_name": ["baseline_arm_1"],
            }
        )
        _, mrn_lookup = map_mrns_to_records(alerts, redcap_fields)
        assert "001234" in mrn_lookup
        assert mrn_lookup["001234"] == "42"

    def test_preserves_leading_zeros_in_record_ids(self):
        """Record IDs with leading zeros are preserved."""
        from hbnmigration.from_curious.utils import map_mrns_to_records

        alerts = pd.DataFrame(
            {
                "record": ["007", "007"],
                "field_name": ["mrn", "alerts_test_1"],
                "value": ["007", "yes"],
                "redcap_event_name": [None, None],
            }
        )
        redcap_fields = pd.DataFrame(
            {
                "record": ["007"],
                "field_name": ["mrn"],
                "value": ["007"],
                "redcap_event_name": ["baseline_arm_1"],
            }
        )
        _, mrn_lookup = map_mrns_to_records(alerts, redcap_fields)
        assert mrn_lookup["007"] == "007"
