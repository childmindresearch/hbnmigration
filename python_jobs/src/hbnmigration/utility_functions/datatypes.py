"""Custom datatypes."""

from abc import ABC
from typing import Annotated, Literal, NotRequired, Optional, TypedDict

from pydantic.types import StringConstraints

ApiProtocol = Literal["https", "wss"]
ApiProtocols: list[ApiProtocol] = ["https", "wss"]


class Credentials(ABC):
    """Class to store credentials."""


CuriousId = Annotated[
    str,
    StringConstraints(pattern=r"^[a-zA-Z0-9]{8}-([a-zA-Z0-9]{4}){3}-[a-zA-Z0-9]{12}$"),
]
"""ID string for a Curious entity."""

_iso_8601_pattern = (
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?$"
)
Datetime = Annotated[str, StringConstraints(pattern=_iso_8601_pattern)]
"""ISO 8601 datetime string."""

SemanticVersion = Annotated[
    str, StringConstraints(pattern=r"^v?\d+\.\d+\.\d+([-+]\w+)?$")
]
"""Semver string."""


class CuriousEncryption(TypedDict):
    """Curious encryption data."""

    base: str
    prime: str
    accountId: CuriousId
    publicKey: str


class CuriousAlert(TypedDict):
    """API response from Curious alerts endpoint."""

    id: CuriousId
    isWatched: bool
    appletId: CuriousId
    appletName: str
    version: SemanticVersion
    secretId: str
    activityId: CuriousId
    activityItemId: CuriousId
    message: str
    createdAt: Datetime
    answerId: CuriousId
    encryption: CuriousEncryption
    image: NotRequired[Optional[str]]
    workspace: str
    respondentId: CuriousId
    subjectId: CuriousId
    type: str


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
