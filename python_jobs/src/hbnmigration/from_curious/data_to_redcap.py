"""Send Curious data to REDCap."""

import logging
import os
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
from typing import Literal

import polars as pl
import requests

from mindlogger_data_export.mindlogger import MindloggerData
from mindlogger_data_export.outputs import NamedOutput, RedcapImportFormat

from .._config_variables import curious_variables, redcap_variables
from ..config import Config
from ..utility_functions import (
    CliOptions,
    Endpoints,
    fetch_api_data,
    get_redcap_event_names,
    initialize_logging,
    InstrumentRowCount,
    PROJECT_STATUS,
    Results,
    today,
    tsx,
    yesterday,
)

initialize_logging()
logger = logging.getLogger(__name__)

ENDPOINTS: dict[Literal["Curious", "REDCap"], Endpoints] = {
    "Curious": curious_variables.Endpoints(),
    "REDCap": redcap_variables.Endpoints(),
}
"""Initialized endpoints"""

REDCAP_TOKEN = getattr(
    redcap_variables.Tokens, "pid625" if PROJECT_STATUS == "prod" else "pid744"
)
"""REDCap token for specified project status"""


def format_for_redcap(
    curious_data_dir: Path,
) -> tuple[list[NamedOutput], InstrumentRowCount]:
    """Format Curious data for REDCap."""
    event_names = get_redcap_event_names(
        ENDPOINTS["REDCap"].base_url, redcap_variables.headers, {"token": REDCAP_TOKEN}
    )

    # Create formatter with project name
    formatter = RedcapImportFormat(project=event_names)

    # Process data
    try:
        ml_data = MindloggerData.create(curious_data_dir)
    except pl.exceptions.NoDataError:
        logger.info("No Curious data to export.")
        sys.exit(0)
    outputs = formatter.produce(ml_data)

    logger.info(
        "Data formatted for these instruments: %s",
        "".join([f"\n\t- {_.name[:-7]}" for _ in outputs]),
    )
    return outputs, formatter.get_instrument_row_counts()


def get_curious_data(request_json: CliOptions) -> None:
    """Try to pull Curious data."""
    tsx(
        Config.PROJECT_ROOT / "javascript_jobs/autoexport/src/index.ts",
        request_json.long.split(" "),
        parse_output=False,
    )


def push_to_redcap(csv_path: Path) -> None:
    """Push data to RedCap."""
    if csv_path.stat().st_size:
        with csv_path.open("r") as csv_content:
            data = {
                "token": REDCAP_TOKEN,
                "content": "record",
                "action": "import",
                "format": "csv",
                "type": "flat",
                "overwriteBehavior": "normal",
                "forceAutoNumber": "false",
                "data": csv_content.read(),  # Send to RedCap cvs file content
                "returnContent": "count",
                "returnFormat": "csv",
            }
            r = requests.post(ENDPOINTS["REDCap"].base_url, data=data)
            if r.status_code != requests.codes["okay"]:
                logger.exception(r.reason)
                logger.exception(r.text)
                logger.exception("HTTP Status: %d", r.status_code)
                r.raise_for_status()


def save_for_redcap(outputs: list[NamedOutput], redcap_data_dir: Path):
    """Save REDCap data."""
    # Save outputs

    for output in outputs:
        nested_cols = [
            col
            for col in output.output.columns
            if output.output[col].dtype in [pl.List, pl.Struct]
            or str(output.output[col].dtype).startswith("List")
            or str(output.output[col].dtype).startswith("Struct")
        ]
        if nested_cols:
            logger.info("Output '%s' has nested columns: %s", output.name, nested_cols)
            for col in nested_cols:
                logger.info("  %s: %s", col, output.output[col].dtype)
        output.output.write_csv(
            (redcap_data_dir / output.name.replace("_redcap", "")).with_suffix(".csv")
        )


def send_to_redcap(redcap_path: Path, instrument_row_count: dict[str, int]) -> Results:
    """Send data to REDCap."""
    results = Results()
    instruments: list[str] = [
        instrument.lower()
        for instrument in list(
            fetch_api_data(
                ENDPOINTS["REDCap"].base_url,
                redcap_variables.headers,
                {
                    "token": REDCAP_TOKEN,
                    "content": "instrument",
                    "format": "csv",
                    "returnFormat": "csv",
                },
            )["instrument_name"]
        )
    ]
    to_send = [
        instrument
        for instrument in redcap_path.iterdir()
        if instrument.stem.lower() in list(instruments)
    ]
    logger.info(
        "Ready to send to REDCap: %s",
        "".join([f"\n\t- {file.stem}" for file in to_send]),
    )
    for instrument in to_send:
        try:
            push_to_redcap(instrument)
            results.success += instrument_row_count.get(instrument.stem, 0)
        except Exception:
            logger.exception("%s\n", instrument)
            results.failure.append(instrument.stem)
    return results


def main() -> None:
    """Send Curious data to REDCap."""
    request_json = CliOptions({"fromDate": yesterday, "toDate": today})
    """All data from yesterday to now."""

    with TemporaryDirectory() as curious_temp_data_dir:
        applet_credentials = curious_variables.AppletCredentials.hbn_mindlogger[
            "Healthy Brain Network Questionnaires"
        ]
        os.environ.update(
            {key.upper(): value for key, value in applet_credentials.items()}
        )
        root_temp_path = Path(curious_temp_data_dir)
        data_dir_paths = {
            source: root_temp_path / f"from_{source}"
            for source in ["curious", "redcap"]
        }
        for path in data_dir_paths.values():
            path.mkdir(parents=True, exist_ok=True)
        curious_export_file = data_dir_paths["curious"] / "responses_curious.csv"
        request_json["output"] = str(curious_export_file)
        get_curious_data(request_json)
        outputs, _instrument_row_count = format_for_redcap(data_dir_paths["curious"])
        instrument_row_count: dict[str, int] = {
            k: v for k, v in _instrument_row_count.items() if v is not None
        }
        save_for_redcap(outputs, data_dir_paths["redcap"])
        results = send_to_redcap(data_dir_paths["redcap"], instrument_row_count)
        logger.info(results.report, yesterday)


if __name__ == "__main__":
    main()
