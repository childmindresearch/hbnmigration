"""Curious variables (secret)."""

from typing import Optional
from warnings import deprecated

from ...utility_functions import (
    ApiProtocol,
    Credentials as CredentialsABC,
    CuriousApplets,
    CuriousId,
    Endpoints as EndpointsABC,
    Tokens as TokensABC,
)

applets: CuriousApplets

class _ActivityIds(type):
    """Metaclass for deprecating `applet_ids` module-level global variable."""

    _data: dict[str, str]
    def __getitem__(cls, key: str) -> CuriousId: ...

@deprecated("Deprecated in v1.9.0. Use `applets[applet_name].activities`.")
class activity_ids(metaclass=_ActivityIds):  # noqa: N801
    """Curious activity IDs."""

    @classmethod
    def __getitem__(cls, key: str) -> CuriousId: ...

class _AppletIds(type):
    """Metaclass for deprecating `applet_ids` module-level global variable."""

    _data: dict[str, str]
    def __getitem__(cls, key: str) -> CuriousId: ...

applet_ids: _AppletIds  # type: ignore[no-redef]

@deprecated("Deprecated in v1.9.0. Use `applets`.")
class applet_ids(metaclass=_AppletIds):  # type: ignore[no-redef]  # noqa: N801
    """Curious applet IDs."""

    @classmethod
    def __getitem__(cls, key: str) -> CuriousId: ...

@deprecated("Deprecated in v1.9.0. Use `AppletCredentials`.")
class Credentials(CredentialsABC):
    """API login credentials for 'hbn_mindlogger'."""

    @staticmethod
    @deprecated("Deprecated in v1.9.0. Use `AppletCredentials[applet_name]`.")
    def _hbn_mindlogger() -> dict[str, str]:
        """
        Applet credentials for decryption.

        .. version-deprecated:: 1.9.0
           Use `AppletCredentials[applet_name]`
        """

    hbn_mindlogger: dict[str, str]
    """API login credentials for 'hbn_mindlogger'.

    .. version-deprecated:: 1.9.0
        Use `AppletCredentials[applet_name]`
    """
    _hbn_mindlogger()

class AppletCredentials(CredentialsABC):
    """Applet credentials for decryption."""

    def __init__(self) -> None:
        """Initialize Applet Credentials."""

    @staticmethod
    @deprecated("Deprecated in v1.9.0. Use `AppletCredentials[applet_name]`.")
    def _hbn_mindlogger() -> dict[str, dict[str, str]]:
        """
        Applet credentials for decryption.

        .. version-deprecated:: 1.9.0
           Use `AppletCredentials[applet_name]`
        """

    hbn_mindlogger: dict[str, dict[str, str]]
    """Applet credentials for decryption.

    .. version-deprecated:: 1.9.0
        Use `AppletCredentials[applet_name]`
    """
    _hbn_mindlogger()

    def __getitem__(self, key: str) -> dict[str, str]:
        """Get applet credentials for given applet name."""

class Endpoints(EndpointsABC):
    """Curious endpoints."""

    def __init__(
        self, host: str = "api-v2.gettingcurious.com", protocol: ApiProtocol = "https"
    ) -> None:
        """Initialize Curious API Endpoints."""
        self.host: str
        self.protocol: ApiProtocol

    def activity(self, activity_id: CuriousId) -> str:
        """Endpoint for activities."""

    @property
    def alerts(self) -> str:
        """Endpoint for alerts."""

    def applet(self, applet_id: CuriousId) -> str:
        """Return applet encryption info."""

    def applet_activity_answers_list(
        self, applet_id: CuriousId, activity_id: CuriousId
    ) -> str:
        """Return applet activity answers list endpoint."""

    @property
    def auth(self) -> str:
        """Return authentication endpoint."""

    @property
    def _base_url(self) -> str:
        """Base URL for Curious API calls."""

    @_base_url.setter
    def _base_url(self, url: str) -> None:
        """Raise exception ― _base_url is calculated for Curious endpoints."""

    def invitation_statuses(self, owner_id: CuriousId, applet_id: CuriousId) -> str:
        """Return invitation statuses endpoint."""

    @property
    def login(self) -> str:
        """Curious authentication endpoint."""

def headers(token: Optional[str]) -> dict[str, str]:
    """Return Curious headers."""

owner_ids: dict[str, CuriousId]
"""Curious project owner IDs."""

class Tokens(TokensABC):
    """Curious tokens."""

    def __init__(self, endpoints: Endpoints, credentials: dict[str, str]) -> None:
        """Initialize Curious tokens."""
        self.endpoints: Endpoints
        self.access: str
        self.refresh: str
