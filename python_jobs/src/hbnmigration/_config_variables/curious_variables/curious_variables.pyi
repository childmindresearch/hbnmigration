"""Typestubs for secret Curious variables."""

from ...utility_functions import ApiProtocol
from ...utility_functions import Credentials as CredentialsABC
from ...utility_functions import Endpoints as EndpointsABC
from ...utility_functions import Tokens as TokensABC

class AppletCredentials(CredentialsABC):
    hbn_mindlogger: dict[str, str]

class Credentials(CredentialsABC):
    hbn_mindlogger: dict[str, str]

class Endpoints(EndpointsABC):
    def __init__(self, host: str = ..., protocol: ApiProtocol = ...) -> None: ...
    @property
    def alerts(self) -> str: ...
    def applet_activity_answers_list(self, applet_id: str, activity_id: str) -> str: ...
    @property
    def auth(self) -> str: ...
    @property
    def login(self) -> str: ...

headers: dict[str, str]
owner_ids: dict[str, str]
applet_ids: dict[str, str]

class Tokens(TokensABC):
    access: str
    endpoints: Endpoints
    refresh: str

    def __init__(self, endpoints: Endpoints, credentials: dict[str, str]) -> None: ...
