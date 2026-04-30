"""Custom datatypes."""

from abc import ABC
from collections import UserDict
from collections.abc import ItemsView, KeysView, ValuesView
import json
from typing import (
    Annotated,
    Any,
    cast,
    Iterator,
    Literal,
    NotRequired,
    Optional,
    TypedDict,
    TypeVar,
)
from warnings import warn

from pydantic.types import StringConstraints

ApiProtocol = Literal["https", "wss"]
ApiProtocols: list[ApiProtocol] = ["https", "wss"]

T = TypeVar("T")


def deprecated_module_level(version: str, replacement: str, value: T) -> T:
    """Warn about module-level deprecation and return original value."""
    warning_message = "Deprecated in %s. Use %s."
    warn(
        warning_message % (version, replacement),
        category=DeprecationWarning,
        stacklevel=3,
    )
    return value


class CliOptions(UserDict):
    """Dictionary with CLI string methods."""

    @property
    def long(self) -> str:
        """Return long-form options string."""
        return " ".join(f"--{key} {value}" for key, value in self.data.items())


class ColumnRenameMapping:
    """Base class for column rename mapping configurations."""


class ConstraintABC(ABC):
    """Base class for constraints."""


class Credentials(ABC):
    """Class to store credentials."""


class RangeConstraint(ConstraintABC):
    """Range constraint defining [min, max], (min, max), [min, max), or (min, max]."""

    def __init__(
        self,
        minimum: float,
        maximum: float,
        left_inclusive: bool = True,
        right_inclusive: bool = True,
    ) -> None:
        """Initialize range constraint."""
        self._min = (minimum, left_inclusive)
        self._max = (maximum, right_inclusive)

    def in_range(self, value: float) -> bool:
        """Check if value in range with short-circuiting logic."""
        min_val, left_inc = self._min
        max_val, right_inc = self._max

        # Lower bound check
        lower_ok = (value >= min_val) if left_inc else (value > min_val)
        if not lower_ok:
            return False

        # Upper bound check
        return (value <= max_val) if right_inc else (value < max_val)

    # --- Minimum Properties ---

    @property
    def minimum(self) -> float:
        """Return minimum value."""
        return self._min[0]

    @minimum.setter
    def minimum(self, value: float) -> None:
        """Set minimum value while preserving inclusiveness."""
        self._min = (value, self._min[1])

    @property
    def left_inclusive(self) -> bool:
        """Return True if left bound is inclusive [."""
        return self._min[1]

    @left_inclusive.setter
    def left_inclusive(self, value: bool) -> None:
        """Set whether left bound is inclusive."""
        self._min = (self._min[0], value)

    # --- Maximum Properties ---

    @property
    def maximum(self) -> float:
        """Return maximum value."""
        return self._max[0]

    @maximum.setter
    def maximum(self, value: float) -> None:
        """Set maximum value while preserving inclusiveness."""
        self._max = (value, self._max[1])

    @property
    def right_inclusive(self) -> bool:
        """Return True if right bound is inclusive ]."""
        return self._max[1]

    @right_inclusive.setter
    def right_inclusive(self, value: bool) -> None:
        """Set whether right bound is inclusive."""
        self._max = (self._max[0], value)

    # --- Representations ---

    def __repr__(self) -> str:
        """Return reproducible string representation."""
        return (
            f"RangeConstraint(minimum={self.minimum}, maximum={self.maximum}, "
            f"left_inclusive={self.left_inclusive}, "
            f"right_inclusive={self.right_inclusive})"
        )

    def __str__(self) -> str:
        """Return standard mathematical interval notation (e.g., [0, 11))."""
        left_bracket = "[" if self.left_inclusive else "("
        right_bracket = "]" if self.right_inclusive else ")"
        return f"{left_bracket}{self.minimum}, {self.maximum}{right_bracket}"


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


class CuriousActivityInfo:
    """Information about a Curious activity."""

    def __init__(self, activity_id: "CuriousId", name: Optional[str] = None) -> None:
        """Initialize Curious Activity Info."""
        self.activity_id = activity_id
        if name:
            self.name = name

    def __repr__(self) -> str:
        """Return reproducible string representation of CuriousActivityInfo."""
        return str(self)

    def __str__(self) -> str:
        """Return string representation of CuriousActivityInfo."""
        return f'"{self.name}": CuriousActivityInfo({self.activity_id})'

    @property
    def name(self) -> str:
        """Get applet name."""
        return self._name

    @name.setter
    def name(self, name: str) -> None:
        """Set applet name."""
        self._name = name


class CuriousAppletInfo:
    """Information for a Curious applet."""

    def __init__(
        self,
        applet_id: "CuriousId",
        name: str,
        activities: Optional[list[CuriousActivityInfo]] = None,
    ) -> None:
        """Initialize Curious applet."""
        self.applet_id = applet_id
        self.name = name
        self._activities: dict[str, CuriousActivityInfo] = {}
        if activities:
            for activity in activities:
                self._activities[activity.name] = activity

    def __repr__(self) -> str:
        """Return reproducible string representation of CuriousAppletInfo."""
        return str(self)

    def __str__(self) -> str:
        """Return string representation of CuriousAppletInfo."""
        return f"CuriousAppletInfo({self.applet_id}, {self.name}): {self.activities}"

    @property
    def activities(self) -> dict[str, CuriousActivityInfo]:
        """Get dictionary of activities in a Curious applet, keyed by name."""
        return self._activities

    @activities.setter
    def activities(
        self, activities=CuriousActivityInfo | list[CuriousActivityInfo]
    ) -> None:
        """Set dictionary of activities in a Curious applet, keyed by name."""
        activities_list = cast(
            list[CuriousActivityInfo],
            activities if isinstance(activities, list) else [activities],
        )
        for activity in activities_list:
            self._activities[activity.name] = activity

    @property
    def name(self) -> str:
        """Get applet name."""
        return self._name

    @name.setter
    def name(self, name: str) -> None:
        """Set applet name."""
        self._name = name


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


class CuriousApplets:
    """Curious Applets keyed by name."""

    def __init__(self) -> None:
        """Initialize Curious applets."""
        self._info: dict[str, CuriousAppletInfo] = {}

    def __getitem__(self, key: str) -> CuriousAppletInfo:
        """Get a Curious applet by name."""
        return self.info[key]

    def __setitem__(self, key: str, value: CuriousAppletInfo) -> None:
        """Set a Curious applet by name."""
        self.info[key] = value

    def __repr__(self) -> str:
        """Return reproducible string representation of Applets."""
        return str(self)

    def __str__(self) -> str:
        """Return string representation of Applets."""
        return str(set(self.info.values()))

    def keys(self) -> KeysView[str]:
        """Get defined names of applets."""
        return self.info.keys()

    @property
    def info(self) -> dict[str, CuriousAppletInfo]:
        """Get dictionary of Curious applets keyed by name."""
        return self._info

    @info.setter
    def info(self, applet: CuriousAppletInfo | list[CuriousAppletInfo]) -> None:
        """Set dictionary of Curious applets keyed by name."""
        if not isinstance(applet, list):
            applet = [applet]
        for info in applet:
            self._info[info.name] = info


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

    _base_url: property | str = NotImplemented
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
Record = dict[str, str | int | list[str | int] | set[str | int] | tuple[str | int, ...]]
"""Record TypeAlias"""


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

    def __eq__(self, other: object) -> bool:
        """Compare two ValueFields."""
        if not isinstance(other, ValueField):
            msg = f"Cannot compare {type[self]} and {type[other]}."
            raise TypeError(msg)
        return (
            self._field_name == other._field_name
            and self._value_dict == other._value_dict
        )

    def __getitem__(self, key) -> str:
        """Allow dict-like access: `field['label']`."""
        return self._value_dict[key]

    def __hash__(self) -> int:
        """ValueField is not hashable."""
        msg = f"unhashable type: '{type(self).__name__}'"
        raise TypeError(msg)

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
