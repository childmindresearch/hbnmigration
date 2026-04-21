"""
Transfer data from one REDCap project to another via webhook triggers.

When ``ready_to_send_to_intake_redcap`` field is set to 1 in
Healthy Brain Network Study Consent (IRB Approved) (PID 247),
copies the approved participants to HBN - Operations and Data Collection (PID 625).

Can also be run manually via CLI to process all pending records::

    python -m hbnmigration.from_redcap.to_redcap
"""

from typing import Annotated, Any, cast

from fastapi import BackgroundTasks, FastAPI, Form, Request
from fastapi.responses import JSONResponse
import pandas as pd
from pydantic import BaseModel, Field
import uvicorn

from .._config_variables import redcap_variables
from ..exceptions import NoData
from ..utility_functions import initialize_logging, redcap_api_push
from .config import Fields, Values
from .from_redcap import fetch_data

logger = initialize_logging(__name__)

_REDCAP_TOKENS = redcap_variables.Tokens()
_REDCAP_ENDPOINTS = redcap_variables.Endpoints()
_SOURCE_PID = 247
_TARGET_PID = 625

app = FastAPI(
    title="REDCap to REDCap Migration Service",
    description=(
        "Handles REDCap Data Entry Triggers for pushing data "
        "from Intake (PID 247) to Operations (PID 625)"
    ),
)


class RedcapTriggerPayload(BaseModel):
    """Payload from REDCap Data Entry Trigger."""

    project_id: int
    instrument: str
    record: str
    redcap_event_name: str | None = None
    redcap_repeat_instance: int | None = Field(None, alias="redcap_repeat_instance")
    redcap_repeat_instrument: str | None = Field(None, alias="redcap_repeat_instrument")
    redcap_data_access_group: str | None = Field(None, alias="redcap_data_access_group")
    redcap_url: str | None = Field(None, alias="redcap_url")
    project_url: str | None = Field(None, alias="project_url")
    username: str | None = None
    ready_to_send_to_intake_redcap: str | None = Field(
        None, alias="ready_to_send_to_intake_redcap"
    )

    class Config:
        """Pydantic config."""

        populate_by_name = True


def event_map(redcap_data: pd.DataFrame) -> dict[str, str]:
    """
    Build a mapping from ``field_name`` to ``redcap_event_name``.

    In a longitudinal REDCap project, each field belongs to a specific event.
    This extracts that pairing from the fetched data.

    Returns:
        A dict of {field_name: redcap_event_name}.

    """
    return cast(
        dict[str, str],
        redcap_data[["field_name", "redcap_event_name"]]
        .drop_duplicates()
        .set_index("field_name")["redcap_event_name"]
        .to_dict(),
    )


def format_data_for_redcap_operations(redcap_data: pd.DataFrame) -> pd.DataFrame:
    """
    Format data from intake (PID 247) for operations (PID 625).

    Parameters
    ----------
    redcap_data
        DataFrame with columns [record, field_name, value, redcap_event_name]

    Returns
    -------
    Formatted DataFrame ready for import to Operations REDCap.

    """
    df = redcap_data.copy()
    if hasattr(Fields, "export_247") and hasattr(
        Fields.export_247, "for_redcap_operations"
    ):
        intake_fields = str(Fields.export_247.for_redcap_operations).split(",")
        df = df[df["field_name"].isin(intake_fields)]
    return df


def push_to_intake_redcap(source_data: pd.DataFrame) -> int:
    """
    Push data to operations (PID 625).

    Args:
        source_data: DataFrame with formatted data to push.

    Returns:
        Number of rows successfully pushed.

    """
    try:
        rows_updated = redcap_api_push(
            df=source_data,
            token=_REDCAP_TOKENS.pid625,
            url=_REDCAP_ENDPOINTS.base_url,
            headers=redcap_variables.headers,
        )
        logger.info(
            "%d rows successfully pushed to Operations REDCap (PID %d).",
            rows_updated,
            _TARGET_PID,
        )
        return rows_updated
    except Exception:
        logger.exception(
            "Failed to push data to Operations REDCap (PID %d)", _TARGET_PID
        )
        raise


def update_source_redcap_status(
    record_id: str,
    status_value: str,
    event_name: str,
) -> None:
    """
    Update Intake (PID 247) with the push status.

    Args:
        record_id: The record ID to update.
        status_value: The status value to set.
        event_name: The event name for the status field.

    """
    try:
        update_data = pd.DataFrame(
            [
                {
                    "record": record_id,
                    "redcap_event_name": event_name,
                    "field_name": "ready_to_send_to_intake_redcap",
                    "value": status_value,
                }
            ]
        )
        redcap_api_push(
            df=update_data,
            token=_REDCAP_TOKENS.pid247,
            url=_REDCAP_ENDPOINTS.base_url,
            headers=redcap_variables.headers,
        )
        logger.info(
            "Updated status for record %s in PID %d to '%s'",
            record_id,
            _SOURCE_PID,
            status_value,
        )
    except Exception:
        logger.exception(
            "Failed to update source REDCap status for record %s", record_id
        )
        raise


def clear_ready_flag(record_id: str) -> None:
    """
    Clear the ready-to-send flag in Intake (PID 247).

    Args:
        record_id: The record ID to update.

    """
    try:
        data = fetch_data(
            _REDCAP_TOKENS.pid247,
            "ready_to_send_to_intake_redcap",
            filter_logic=f"[record_id] = '{record_id}'",
        )
        if data.empty:
            logger.warning(
                "Could not find record %s in PID %d to clear flag",
                record_id,
                _SOURCE_PID,
            )
            return
        event = event_map(data).get("ready_to_send_to_intake_redcap")
        if event is None:
            logger.error(
                "Could not determine redcap_event_name for "
                "'ready_to_send_to_intake_redcap'. "
                "Skipping flag clear for record %s.",
                record_id,
            )
            return
        update_source_redcap_status(record_id, "0", event)
    except Exception:
        logger.exception("Error clearing ready flag for record %s", record_id)


def process_record_for_redcap_operations(record_id: str) -> dict[str, Any]:
    """
    Process a single record triggered by REDCap webhook.

    Args:
        record_id: The record ID from the trigger.

    Returns:
        Dictionary with status and message.

    """
    try:
        logger.info("Processing record %s for Intake REDCap push", record_id)
        if hasattr(Fields, "export_247") and hasattr(
            Fields.export_247, "for_redcap_operations"
        ):
            fields_to_export = str(Fields.export_247.for_redcap_operations)
        else:
            fields_to_export = None

        source_data = fetch_data(
            _REDCAP_TOKENS.pid247,
            fields_to_export,
            filter_logic=f"[record_id] = '{record_id}'",
        )
        if source_data.empty:
            logger.warning(
                "No data found for record %s in PID %d", record_id, _SOURCE_PID
            )
            return {
                "status": "error",
                "record_id": record_id,
                "message": f"No data found in Intake (PID {_SOURCE_PID})",
            }

        formatted_data = format_data_for_redcap_operations(source_data)
        if formatted_data.empty:
            logger.info("No processable data for record %s", record_id)
            return {
                "status": "error",
                "record_id": record_id,
                "message": "No processable data after formatting",
            }

        rows_pushed = push_to_intake_redcap(formatted_data)
        clear_ready_flag(record_id)

        return {
            "status": "success",
            "record_id": record_id,
            "message": f"Pushed {rows_pushed} row(s) to Operations (PID {_TARGET_PID})",
            "rows_pushed": rows_pushed,
        }
    except Exception as e:
        logger.exception("Error processing record %s for Operations REDCap", record_id)
        return {
            "status": "error",
            "record_id": record_id,
            "message": str(e),
        }


# ---------------------------------------------------------------------------
# Webhook endpoints
# ---------------------------------------------------------------------------


@app.get("/")
async def root() -> dict[str, str]:
    """Root endpoint."""
    return {"message": "REDCap to REDCap Migration Service"}


@app.get("/health")
async def health() -> dict[str, str]:
    """Health check endpoint."""
    return {"status": "healthy"}


@app.post("/webhook/redcap-to-intake")
async def redcap_to_intake_webhook(
    background_tasks: BackgroundTasks,
    instrument: Annotated[str, Form()],
    record: Annotated[str, Form()],
    ready_to_send_to_intake_redcap: Annotated[str | None, Form()] = None,
) -> dict[str, Any]:
    """
    Handle REDCap Data Entry Trigger for Intake updates.

    This endpoint should be configured as a Data Entry Trigger in
    REDCap PID 247. When ``ready_to_send_to_intake_redcap`` is set to ``1``,
    REDCap will POST to this endpoint.

    Configuration in REDCap:

    1. Go to Project Setup → Additional Customizations
    2. Enable "Data Entry Trigger"
    3. Set URL to: ``https://your-domain.com/webhook/redcap-to-intake``

    """
    logger.info(
        "Received REDCap trigger for record %s (instrument: %s)",
        record,
        instrument,
    )
    if ready_to_send_to_intake_redcap != "1":
        logger.debug("Ready flag not set for record %s, ignoring trigger", record)
        return {
            "status": "ignored",
            "message": "Ready flag not set to '1', ignoring trigger",
            "record_id": record,
        }
    background_tasks.add_task(process_record_for_redcap_operations, record)
    logger.info("Queued record %s for Intake REDCap push", record)
    return {
        "status": "accepted",
        "message": f"Trigger accepted for record {record}",
        "record_id": record,
    }


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """Global exception handler."""
    logger.exception("Unhandled exception in REDCap to REDCap service")
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error"},
    )


# ---------------------------------------------------------------------------
# CLI entry points
# ---------------------------------------------------------------------------


def main() -> None:
    """
    Process all pending 'Ready to Send to Intake Redcap' records (bulk/manual).

    This is the batch-processing entry point. Run manually or via cron to
    process every record currently flagged in REDCap PID 247.

    Usage::

        python -m hbnmigration.from_redcap.to_redcap

    """
    try:
        if hasattr(Fields, "export_247") and hasattr(
            Fields.export_247, "for_redcap_operations"
        ):
            fields_to_export = str(Fields.export_247.for_redcap_operations)
        else:
            fields_to_export = None

        data_operations = fetch_data(
            _REDCAP_TOKENS.pid247,
            fields_to_export,
            Values.PID247.intake_ready.filter_logic("Ready to Send to Intake Redcap"),
        )
        if data_operations.empty:
            logger.info(
                "REDCap PID %s: No participants marked "
                "'Ready to Send to Intake Redcap'.",
                _SOURCE_PID,
            )
            raise NoData
    except NoData:
        logger.info(
            "No data to transfer from PID %s to PID %s.",
            _SOURCE_PID,
            _TARGET_PID,
        )
        return

    formatted_data = format_data_for_redcap_operations(data_operations)
    if formatted_data.empty:
        logger.info("No data to push after formatting.")
        return

    try:
        push_to_intake_redcap(formatted_data)
    except Exception:
        logger.exception("Batch push to Intake REDCap failed.")
        return

    pushed_records = set(formatted_data["record"].unique())
    for record_id in pushed_records:
        clear_ready_flag(str(record_id))


def serve(host: str = "0.0.0.0", port: int = 8001) -> None:
    """
    Start the webhook server.

    Args:
        host: Bind address.
        port: Bind port.

    """
    uvicorn.run(app, host=host, port=port)


if __name__ == "__main__":
    main()
