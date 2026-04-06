"""Configuration loaded from environment variables."""

import os
from pathlib import Path
from typing import cast

from .utility_functions.datatypes import ProjectStatus, ProjectStatuses


def _get_project_status() -> ProjectStatus:
    """Get and validate project status from environment."""
    _project_status = os.environ.get("HBNMIGRATION_PROJECT_STATUS", "prod")
    if _project_status not in ProjectStatuses:
        msg = (
            f"${{HBNMIGRATION_PROJECT_STATUS}} must be one of {ProjectStatuses} but is "
            f"set to {_project_status}"
        )
        raise ValueError(msg)
    return cast(ProjectStatus, _project_status)


class Config:
    """Configuration loaded from environment variables."""

    PROJECT_ROOT = (
        Path(os.environ["HBNMIGRATION_PROJECT_ROOT"])
        if "HBNMIGRATION_PROJECT_ROOT" in os.environ
        else NotImplemented
    )
    """Path to root of hbnmigration project on server."""

    LOG_ROOT: Path = (
        Path(os.environ["HBNMIGTRATION_LOG_ROOT"])
        if "HBNMIGRATION_LOG_ROOT" in os.environ
        else PROJECT_ROOT / ".logs"
    )
    """Path to logging root."""

    PROJECT_STATUS: ProjectStatus = _get_project_status()
    """
    Project status.

    dev = HBN - Intake and Curious (TEMP for Transition) PID 744
    prod = HBN - Operations and Data Collection PID 625
    """


__all__ = ["Config"]
