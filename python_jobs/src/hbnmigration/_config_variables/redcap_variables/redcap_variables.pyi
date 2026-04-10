"""Typestubs for secret REDCap variables."""

from pathlib import Path

from ...utility_functions import Endpoints as EndpointsABC

class Endpoints(EndpointsABC):
    def __init__(self) -> None: ...
    @property
    def base_url(self) -> str: ...

headers: dict[str, str]

class Tokens:
    @property
    def pid247(self) -> str: ...
    @property
    def pid625(self) -> str: ...
    @property
    def pid744(self) -> str: ...
    @property
    def pid757(self) -> str: ...

redcap_import_file: Path
redcap_update_file: Path
