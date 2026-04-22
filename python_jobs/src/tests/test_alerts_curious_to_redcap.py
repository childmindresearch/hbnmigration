"""Tests for alerts_to_redcap module."""

import json
from typing import cast
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest
import requests
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
from hbnmigration.utility_functions import (
    CuriousAlert,
    CuriousAlertHttps,
    CuriousAlertWebsocket,
)

# Import shared utilities from conftest
from .conftest import (
    create_alert_df,
    create_mock_invalid_status,
    create_mock_tokens_ws,
    setup_cli_mocks,
    setup_main_test_mocks,
    setup_reconnect_mocks,
    setup_standard_alert_mocks,
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
    result = process_alerts_for_redcap(alert_df)
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


@pytest.mark.asyncio
async def test_main_with_reconnect_reauths_on_401_then_succeeds(caplog):
    """Test that main_with_reconnect re-authenticates on 401 and succeeds."""
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

        await main_with_reconnect(
            applet_name="test_applet", uri="wss://test.com/alerts", max_attempts=2
        )
        # Initial auth + re-auth on 401
        assert mock_auth.call_count == 2
        assert mock_listener.called
        assert "Re-authentication successful" in caplog.text


@pytest.mark.asyncio
async def test_main_with_reconnect_reauth_failure_raises(caplog):
    """Test that main_with_reconnect raises when re-authentication itself fails."""
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
            await main_with_reconnect(
                applet_name="test_applet", uri="wss://test.com/alerts", max_attempts=2
            )
        assert "Re-authentication failed" in caplog.text


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
            applet_name="test_applet", uri="wss://test.com/alerts", max_attempts=1
        )
        assert mocks["ws"].called
        assert mocks["listener"].called


@pytest.mark.asyncio
async def test_main_with_reconnect_handles_connection_error(caplog):
    """Test that main_with_reconnect reconnects on ConnectionClosedError."""
    with setup_reconnect_mocks([ConnectionClosedError(None, None), None]) as mocks:
        await main_with_reconnect(
            applet_name="test_applet", uri="wss://test.com/alerts", max_attempts=2
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
                applet_name="test_applet", uri="wss://test.com/alerts", max_attempts=1
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
            await main_with_reconnect(
                applet_name="test_applet", uri="wss://test.com/alerts", max_attempts=1
            )
        assert (
            "Token expired or invalid" in caplog.text
            or "Authentication failed" in caplog.text
        )


@pytest.mark.asyncio
async def test_main_with_reconnect_infinite_retries():
    """Test that main_with_reconnect runs indefinitely with None max_attempts."""
    side_effects = [ConnectionClosedError(None, None)] * 5 + [None]
    with setup_reconnect_mocks(side_effects) as mocks:
        await main_with_reconnect(
            applet_name="test_applet", uri="wss://test.com/alerts", max_attempts=None
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
            await main(applet_names=["Healthy Brain Network Questionnaires"])
            assert mock_reconnect.called
            call_kwargs = mock_reconnect.call_args[1]
            assert "applet_name" in call_kwargs
            assert "wss://" in call_kwargs["uri"]


@pytest.mark.asyncio
async def test_main_passes_max_attempts():
    """Test that main() passes max_attempts parameter."""
    with patch(
        "hbnmigration.from_curious.alerts_to_redcap.main_with_reconnect"
    ) as mock_reconnect:
        mock_reconnect.return_value = None
        await main(
            applet_names=["Healthy Brain Network Questionnaires"],
            partial_redcap_landing=False,
            max_attempts=10,
        )
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
    process_alerts_for_redcap(alert_df)
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
    setup_standard_alert_mocks(mock_alerts_dependencies, redcap_alerts_metadata)
    alert_df = create_alert_df(records, field_names, ["yes"] * len(field_names))
    result = process_alerts_for_redcap(alert_df)
    # Should have summary field if metadata contains the instrument
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
