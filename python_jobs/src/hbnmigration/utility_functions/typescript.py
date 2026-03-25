"""Utility functions to run TypeScript."""

import json
import subprocess
from typing import Any, cast, Optional

from ..config import Config
from .datatypes import CuriousDecryptedAnswer


def transform_to_report_csv(
    decrypted_answers: list[CuriousDecryptedAnswer],
    enable_data_export_renaming: bool = False,
    raw_answers_object: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Transform decrypted answers to report CSV format using TypeScript."""
    # Prepare input data
    input_data = {
        "answers": decrypted_answers,
        "enableDataExportRenaming": enable_data_export_renaming,
        "rawAnswersObject": raw_answers_object or {},
    }

    return cast(
        list[dict[str, Any]],
        tsx(
            str(
                Config.PROJECT_ROOT
                / "javascript_jobs/autoexport/src/transformReport.ts"
            ),
            _input=json.dumps(input_data, ensure_ascii=False),
        ),
    )


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
    result.check_returncode()
    return json.loads(result.stdout)
