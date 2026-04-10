"""Tests for Microsoft Teams integration."""

from unittest.mock import Mock, patch

import pytest
import requests

from hbnmigration._config_variables.teams_variables import Webhooks
from hbnmigration.utility_functions.teams import send_alert


def test_webhooks_has_failure_channel():
    """Test that Webhooks has the failure alert channel."""
    webhooks = Webhooks()
    assert "Send webhook alerts to 🔴 MS Fabric - Failures" in webhooks.links
    assert webhooks.links["Send webhook alerts to 🔴 MS Fabric - Failures"].startswith(
        "https://"
    )


def test_send_alert_success():
    """Test successful alert sending."""
    with patch("hbnmigration.utility_functions.teams.io.requests.post") as mock_post:
        mock_response = Mock()
        mock_response.status_code = 200
        mock_post.return_value = mock_response

        send_alert("Test message", "Send webhook alerts to 🔴 MS Fabric - Failures")

        assert mock_post.called
        call_args = mock_post.call_args
        assert call_args[1]["json"]["text"] == "Test message"
        assert call_args[1]["timeout"] == 5


def test_send_alert_raises_on_error():
    """Test that send_alert raises on HTTP error."""
    with patch("hbnmigration.utility_functions.teams.io.requests.post") as mock_post:
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.raise_for_status.side_effect = requests.HTTPError("Server error")
        mock_post.return_value = mock_response

        with pytest.raises(requests.HTTPError):
            send_alert("Test", "Send webhook alerts to 🔴 MS Fabric - Failures")


def test_send_alert_invalid_channel():
    """Test that send_alert raises KeyError for invalid channel."""
    with pytest.raises(KeyError):
        send_alert("Test message", "nonexistent_channel")


# test_logging.py additions
"""Additional tests for logging functionality."""
