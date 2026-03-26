"""Typestubs for secret Ripple variables."""

from pathlib import Path
from typing import Literal, Mapping

import pandas as pd

from ...utility_functions import Endpoints as EndpointsABC

class Endpoints(EndpointsABC):
    def __init__(self, host: str = ...) -> None: ...
    def import_data(self, study_id: str) -> str: ...
    @property
    def export_data(self) -> str: ...
    def export_from_ripple(
        self, study_id: str, api_data: dict[str, str] | Mapping[str, str]
    ) -> pd.DataFrame: ...

def column_dict(columns: list[str]) -> dict[str, Literal["on"]]: ...

_token: str

headers: dict[str, dict[str, str]]
study_ids: dict[str, str]

ripple_import_file: Path
