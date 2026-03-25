"""Configuration loaded from environment variables."""

import os
from pathlib import Path


class Config:
    """Configuration loaded from environment variables."""

    PROJECT_ROOT = (
        Path(os.environ["HBNMIGRATION_PROJECT_ROOT"])
        if "HBNMIGRATION_PROJECT_ROOT" in os.environ
        else NotImplemented
    )
    """Path to root of hbnmigration project on server."""
