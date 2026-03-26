"""Typestubs for secret REDCap variables."""

from pathlib import Path

from ...utility_functions import Endpoints as EndpointsABC

class Endpoints(EndpointsABC):
    def __init__(self) -> None: ...
    @property
    def base_url(self) -> str: ...

headers: dict[str, str]

class Tokens:
    pid247: str
    pid625: str
    pid744: str
    pid757: str

redcap_import_file: Path
redcap_update_file: Path
