"""Custom datatypes."""

from abc import ABC
from collections import UserDict
from collections.abc import ItemsView, KeysView, ValuesView
import json
from typing import Annotated, Any, Iterator, Literal, NotRequired, Optional, TypedDict

from pydantic.types import StringConstraints

ApiProtocol = Literal["https", "wss"]
ApiProtocols: list[ApiProtocol] = ["https", "wss"]


class CliOptions(UserDict):
    """Dictionary with CLI string methods."""

    @property
    def long(self) -> str:
        """Return long-form options string."""
        return " ".join(f"--{key} {value}" for key, value in self.data.items())


class Credentials(ABC):
    """Class to store credentials."""


class CuriousActivity(TypedDict):
    """Curious activity."""

    name: str
    description: str
    splashScreen: Any
    image: str
    showAllAtOnce: bool
    isSkippable: bool
    isReviewable: bool
    responseIsEditable: bool
    isHidden: Optional[bool]
    scoresAndReports: dict
    subscaleSetting: dict
    reportIncludedItemName: Optional[bool]
    performanceTaskType: Optional[str]
    isPerformanceTask: bool
    autoAssign: Optional[bool]
    id: "CuriousId"
    order: int
    items: Any
    createdAt: "Datetime"


class CuriousAppletEncryption(TypedDict):
    """
    Encryption info for a Curious applet.

    "encryption": {
      "publicKey": "string",
      "prime": "string",
      "base": "string",
      "accountId": "string"
    },
    """

    accountId: "CuriousId"
    base: str
    prime: str
    publicKey: str


class CuriousAnswer(TypedDict):
    """Encrypted answer from Curious API."""

    activityId: "CuriousId"
    activityHistoryId: "CuriousId"
    answerId: "CuriousId"
    createdAt: "Datetime"
    endDatetime: "Datetime"
    flowHistoryId: "Optional[CuriousId]"
    id: "CuriousId"
    identifier: Optional[str]
    itemIds: "list[CuriousId]"
    items: list[dict]
    migratedData: Optional[Any]
    reviewCount: dict
    sourceSubject: dict
    startDatetime: "Datetime"
    submitId: "CuriousId"
    subscaleSetting: Optional[dict]
    version: "SemanticVersion"
    userPublicKey: str


class CuriousDecryptedAnswer(CuriousAnswer):
    """Encrypted answer from Curious API."""

    answer: list[dict]
    events: list[dict]


class CuriousEncryptedAnswer(CuriousAnswer):
    """Encrypted answer from Curious API."""

    answer: str
    events: str


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


class _CuriousEncryption(TypedDict):
    """Curious encryption data base format."""

    base: str
    prime: str


class CuriousEncryption(_CuriousEncryption):
    """Curious encryption data as returned by HTTPS API."""

    accountId: CuriousId
    publicKey: str


class CuriousEncryptionWebsocket(_CuriousEncryption):
    """Curious encryption data as returned by websocket API."""

    account_id: CuriousId
    public_key: str


class _CuriousAlert(TypedDict):
    """Base API response from Curious alerts endpoint."""

    id: CuriousId
    version: SemanticVersion
    message: str
    image: NotRequired[Optional[str]]
    workspace: str
    type: str


class CuriousAlertHttps(_CuriousAlert):
    """API response from Curious alerts HTTPS endpoint."""

    activityId: CuriousId
    activityItemId: CuriousId
    answerId: CuriousId
    appletId: CuriousId
    appletName: str
    createdAt: Datetime
    encryption: CuriousEncryption
    isWatched: bool
    respondentId: CuriousId
    secretId: str
    subjectId: CuriousId


class CuriousAlertWebsocket(_CuriousAlert):
    """API response from Curious alerts websocket endpoint."""

    activity_id: CuriousId
    activity_item_id: CuriousId
    answer_id: CuriousId
    applet_id: CuriousId
    applet_name: str
    created_at: Datetime
    encryption: CuriousEncryptionWebsocket
    is_watched: bool
    respondent_id: CuriousId
    secret_id: str
    subject_id: CuriousId


CuriousAlert = CuriousAlertHttps | CuriousAlertWebsocket
"""API response from Curious alerts from any connection type."""


class CuriousItem(TypedDict):
    """Curious item."""

    question: str | dict
    responseType: str
    responseValues: dict
    config: dict
    name: str
    isHidden: Optional[bool]
    conditionalLogic: Optional[dict]
    allowEdit: Optional[bool]
    id: CuriousId


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

    def invitation_statuses(self, owner_id: str, applet_id: str) -> str:
        """Return applet activity answers list endpoint."""
        return NotImplemented

    @property
    def login(self) -> str:
        """Authentication endpoint."""
        return NotImplemented


class Tokens:
    """Class to store tokens."""


class FieldDescriptor(UserDict):
    """Descriptor that creates a ValueField instance with the field name."""

    def __init__(self, value_dict: dict[str, str]) -> None:
        """Initialize _FieldDescriptor."""
        self.value_dict = value_dict
        self.field_name = None

    def __set_name__(self, owner, name) -> None:
        """Set field name."""
        self.field_name = name

    def __get__(self, obj, owner) -> "ValueField":
        """Return ValueField."""
        if not self.field_name:
            raise AttributeError
        return ValueField(self.field_name, self.value_dict)


InstrumentRowCount = dict[str, int | None]
ProjectStatus = Literal["dev", "prod"]
ProjectStatuses: list[ProjectStatus] = ["dev", "prod"]


class Results:
    """Class to pass results to data pipeline."""

    def __init__(self) -> None:
        """Initialize results class."""
        self.success: int = 0
        self.failure: list[str] = []

    @property
    def exit_string(self) -> str:
        """Return a JSON dump of the results."""
        return json.dumps(
            {
                "status": self._status,
                "successes": self.success,
                "failures": len(self.failure),
            }
        )

    @property
    def report(self) -> str:
        """
        Report results.

        Usage
        -----
        >>> from logging import getLogger
        >>> from hbnmigration.utility_functions.cache import YESTERDAY
        >>> results = Results()
        >>> getLogger(__name__).info(results.report, YESTERDAY)
        """
        if self.success:
            return f"{self.success} rows submitted to REDCap for %s."
        if self._status == "failure":
            return (
                f"These assessments failed: {self.failure}\nCurious to REDCap transfer "
                "for %s failed. See logs for details."
            )
        return "Curious to REDCap transfer for %s succeeded."

    @property
    def _status(self) -> Literal["no data", "success", "failure"]:
        """Return string representation of status."""
        if len(self.failure):
            return "failure"
        if self.success == 0:
            return "no data"
        return "success"


class ValueField:
    """A field with values and filter logic generation."""

    def __init__(self, field_name: str, value_dict: dict[str, str]) -> None:
        """Initialize a `ValueField`."""
        self._field_name = field_name
        self._value_dict = value_dict

    def filter_logic(self, label: str) -> str:
        """Generate REDCap filter logic for a given label."""
        value = self._value_dict[label]
        return f"[{self._field_name}] = '{value}'"

    def __getitem__(self, key) -> str:
        """Allow dict-like access: `field['label']`."""
        return self._value_dict[key]

    def __iter__(self) -> Iterator[str]:
        """Allow iteration over the dict."""
        return iter(self._value_dict)

    def __repr__(self) -> str:
        """Return reproducible string representation."""
        return f"ValueField({self._field_name}, {self})"

    def __str__(self) -> str:
        """Return string representation."""
        return str(self._value_dict)

    def items(self) -> ItemsView[str, str]:
        """Return ValueField items."""
        return self._value_dict.items()

    def keys(self) -> KeysView[str]:
        """Return ValueField keys."""
        return self._value_dict.keys()

    def values(self) -> ValuesView[str]:
        """Return ValueField values."""
        return self._value_dict.values()


class ValueClass:
    """Base class for value classes."""
