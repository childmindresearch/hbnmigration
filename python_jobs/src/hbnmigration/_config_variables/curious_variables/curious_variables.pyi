"""Typestubs for secret Curious variables."""

from typing import Optional

from ...utility_functions import (
    ApiProtocol,
    Credentials as CredentialsABC,
    CuriousId,
    Endpoints as EndpointsABC,
    Tokens as TokensABC,
)

class AppletCredentials(CredentialsABC):
    hbn_mindlogger: dict[str, str]

class Credentials(CredentialsABC):
    hbn_mindlogger: dict[str, str]

class Endpoints(EndpointsABC):
    def __init__(self, host: str = ..., protocol: ApiProtocol = ...) -> None: ...
    @property
    def alerts(self) -> str: ...
    def applet_activity_answers_list(
        self, applet_id: CuriousId, activity_id: CuriousId
    ) -> str: ...
    @property
    def auth(self) -> str: ...
    def invitation_statuses(self, owner_id: CuriousId, applet_id: CuriousId) -> str: ...
    @property
    def login(self) -> str: ...

activity_ids: dict[str, CuriousId]
applet_ids: dict[str, CuriousId]
owner_ids: dict[str, CuriousId]

def headers(token: Optional[str] = None) -> dict[str, str]: ...

class Tokens(TokensABC):
    access: str
    endpoints: Endpoints
    refresh: str

    def __init__(self, endpoints: Endpoints, credentials: dict[str, str]) -> None: ...
