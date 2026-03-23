"""Custom datatypes."""

from abc import ABC
from typing import Literal

ApiProtocol = Literal["https", "wss"]


class Credentials(ABC):
    """Class to store credentials."""


class Endpoints(ABC):
    """Class to store endpoints."""

    _base_url: str | property = NotImplemented
    """Base URL."""
    host: str = NotImplemented
    """Host address."""
    protocol: ApiProtocol = "https"
    """API protocol."""

    @property
    def alerts(self) -> str:
        """Endpoint for alerts."""
        return NotImplemented

    def applet_activity_answers_list(self, applet_id: str, activity_id: str) -> str:
        """Return applet activity answers list endpoint."""
        return NotImplemented

    @property
    def base_url(self) -> str:
        """Return base URL."""
        return self._base_url

    @property
    def login(self) -> str:
        """Authentication endpoint."""
        return NotImplemented


class Tokens:
    """Class to store tokens."""

    ...
