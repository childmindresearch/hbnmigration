"""Microsoft Teams io."""

import requests

from ..._config_variables.teams_variables import Webhooks


def send_alert(message: str, channel: str) -> None:
    """Send an alert from hbnmigration to Microsoft Teams."""
    payload = {"text": message}

    response = requests.post(Webhooks().links[channel], json=payload, timeout=5)
    response.raise_for_status()
