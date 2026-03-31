"""Utility functions to run TypeScript."""

import json
import logging
import subprocess
from typing import Optional

from .logging import initialize_logging

initialize_logging()
logger = logging.getLogger(__name__)


def tsx(
    script: str, *args, _input: Optional[str] = None
) -> list | dict | str | int | float:
    """Run tsx."""
    result = subprocess.run(
        ["npx", "tsx", script, *args],
        capture_output=True,
        input=_input,
        text=True,
        check=False,
    )

    # Log stderr (debug messages)
    if result.stderr:
        logger.debug("TypeScript stderr: %s", result.stderr)

    # Check for errors first
    try:
        result.check_returncode()
    except subprocess.CalledProcessError as cpe:
        logger.exception("TypeScript error: %s", cpe.stderr)
        raise

    # Debug: Check what we got back
    logger.debug("TypeScript stdout length: %d bytes", len(result.stdout))
    logger.debug("TypeScript stdout (first 500 chars): %s", result.stdout[:500])

    # Check if stdout is empty
    if not result.stdout.strip():
        logger.exception("TypeScript returned empty stdout")
        logger.exception("stderr was: %s", result.stderr)
        msg = "TypeScript script returned no output"
        raise ValueError(msg)

    # Try to parse JSON
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError:
        logger.exception("Output was: %s", {result.stdout[:1000]})
        raise
