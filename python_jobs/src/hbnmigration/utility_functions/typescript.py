"""Utility functions to run TypeScript."""

import json
import logging
from pathlib import Path
import subprocess
from typing import Literal, Optional

from .logging import initialize_logging

initialize_logging()
logger = logging.getLogger(__name__)


def tsx(
    script: str | Path,
    script_args: Optional[list[str]] = None,
    parse_output: Literal[False, "json"] = "json",
    *,
    _input: Optional[str] = None,
) -> list | dict | str | int | float:
    """
    Run tsx.

    Parameters
    ----------
    script
        path to TypeScript script to run
    script_args
        list of commandline arguments for script
    parse_output
        `'json'` to parse JSON output; `False` to return raw STDOUT
    _input
        STDIN for commandline input

    """
    if not script_args:
        script_args = []
    arg_list = ["npx", "tsx", str(script), *script_args]
    logger.info("Calling %s", f"`{' '.join(arg_list)}`")
    result = subprocess.run(
        arg_list,
        capture_output=True,
        input=_input,
        text=True,
        check=False,
    )

    if result.stdout:
        logger.info("[TS]: %s", result.stdout)
    if result.stderr:
        logger.info("[TS]: %s", result.stderr)

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

    if parse_output == "json":
        # Try to parse JSON
        try:
            return json.loads(result.stdout)
        except json.JSONDecodeError:
            logger.exception("Output was: %s", {result.stdout[:1000]})
            raise
    return result.stdout
