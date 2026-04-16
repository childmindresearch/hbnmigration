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
    """Healthy Brain Network Study Consent (IRB Approved) PID 247."""
    pid625: str
    """HBN - Operations and Data Collection PID 625"""
    pid744: str
    """HBN - Intake and Curious (TEMP for Transition) PID 744."""
    pid757: str
    """SANDBOX - Healthy Brain Network Study Consent (IRB Approved) PID 757."""
    pid879: str
    """HBN - Responder Tracking"""

redcap_import_file: Path
redcap_update_file: Path
