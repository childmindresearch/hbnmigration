"""
Transfer data from one REDCap project to another via webhook triggers.

When `ready_to_send_to_intake_redcap` field is set to 1 in REDCap,
copies the approved participants to the Intake REDCap project.
"""

from typing import Annotated, Any, cast

from fastapi import BackgroundTasks, FastAPI, Form, Request
from fastapi.responses import JSONResponse
import pandas as pd
from pydantic import BaseModel, Field

from .._config_variables import redcap_variables
from ..utility_functions import initialize_logging, redcap_api_push
from .config import Fields
from .from_redcap import fetch_data

logger = initialize_logging(__name__)

_REDCAP_TOKENS = redcap_variables.Tokens()
_TARGET_PID = 625

app = FastAPI(
    title="REDCap to REDCap Migration Service",
    description="Handles REDCap Data Entry Triggers for pushing data to Intake REDCap",
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
    Build a mapping from `field_name` to `redcap_event_name`.

    In a longitudinal REDCap project, each field belongs to a specific event.
    This extracts that pairing from the fetched data.
    Returns a dict of {field_name: redcap_event_name}.
    """
    return cast(
        dict[str, str],
        redcap_data[["field_name", "redcap_event_name"]]
        .drop_duplicates()
        .set_index("field_name")["redcap_event_name"]
        .to_dict(),
    )


def format_data_for_intake(redcap_data: pd.DataFrame) -> pd.DataFrame:
    """
    Format data from source REDCap for Intake REDCap.

    Parameters
    ----------
    redcap_data
        DataFrame with columns [record, field_name, value, redcap_event_name]

    Returns
    -------
    Formatted DataFrame ready for import to Intake REDCap.

    """
    # Apply any necessary field name mappings
    df = redcap_data.copy()

    # Filter to only the fields needed for Intake
    if hasattr(Fields, "export_operations") and hasattr(
        Fields.export_operations, "for_intake"
    ):
        intake_fields = str(Fields.export_operations.for_intake).split(",")
        df = df[df["field_name"].isin(intake_fields)]

    return df


def push_to_intake_redcap(
    source_data: pd.DataFrame,
) -> int:
    """
    Push data to Intake REDCap project.

    Args:
        source_data: DataFrame with formatted data to push.

    Returns:
        Number of rows successfully pushed.

    Raises:
        Exception: If push fails.

    """
    try:
        rows_updated = redcap_api_push(
            df=source_data,
            token=_REDCAP_TOKENS.pid247,  # Intake REDCap token
            url=redcap_variables.Endpoints().base_url,
            headers=redcap_variables.headers,
        )
        logger.info(
            "%d rows successfully pushed to Intake REDCap (PID %d).",
            rows_updated,
            _TARGET_PID,
        )
        return rows_updated
    except Exception:
        logger.exception("Failed to push data to Intake REDCap")
        raise


def update_source_redcap_status(
    record_id: str,
    status_value: str,
    event_name: str,
) -> None:
    """
    Update the source REDCap with the push status.

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
            token=_REDCAP_TOKENS.pid625,
            url=redcap_variables.Endpoints().base_url,
            headers=redcap_variables.headers,
        )
        logger.info(
            "Updated status for record %s to '%s'",
            record_id,
            status_value,
        )
    except Exception:
        logger.exception(
            "Failed to update source REDCap status for record %s", record_id
        )
        raise


def clear_ready_flag(record_id: str) -> None:
    """
    Clear the ready-to-send flag after successful push.

    Args:
        record_id: The record ID to update.

    """
    try:
        # Fetch just this record to get the event mapping
        data = fetch_data(
            _REDCAP_TOKENS.pid625,
            "ready_to_send_to_intake_redcap",
            filter_logic=f"[record_id] = '{record_id}'",
        )
        if data.empty:
            logger.warning("Could not find record %s to clear flag", record_id)
            return

        event = event_map(data).get("ready_to_send_to_intake_redcap")
        if event is None:
            logger.exception(
                "Could not determine redcap_event_name for "
                "'ready_to_send_to_intake_redcap'. Skipping flag clear for record %s.",
                record_id,
            )
            return

        update_source_redcap_status(record_id, "0", event)

    except Exception:
        logger.exception("Error clearing ready flag for record %s", record_id)


def process_record_for_intake(record_id: str) -> dict[str, Any]:
    """
    Process a single record triggered by REDCap webhook.

    Args:
        record_id: The record ID from the trigger.

    Returns:
        Dictionary with status and message.

    """
    try:
        logger.info("Processing record %s for Intake REDCap push", record_id)

        # Determine which fields to export
        if hasattr(Fields, "export_operations") and hasattr(
            Fields.export_operations, "for_intake"
        ):
            fields_to_export = str(Fields.export_operations.for_intake)
        else:
            # Export all fields if not specified
            fields_to_export = None

        # Fetch data for this specific record
        source_data = fetch_data(
            _REDCAP_TOKENS.pid625,
            fields_to_export,
            filter_logic=f"[record_id] = '{record_id}'",
        )

        if source_data.empty:
            logger.warning("No data found for record %s", record_id)
            return {
                "status": "error",
                "record_id": record_id,
                "message": "No data found in source REDCap",
            }

        # Format data for Intake
        formatted_data = format_data_for_intake(source_data)

        if formatted_data.empty:
            logger.info("No processable data for record %s", record_id)
            return {
                "status": "error",
                "record_id": record_id,
                "message": "No processable data after formatting",
            }

        # Push to Intake REDCap
        rows_pushed = push_to_intake_redcap(formatted_data)

        # Clear the ready flag
        clear_ready_flag(record_id)

        return {
            "status": "success",
            "record_id": record_id,
            "message": f"Successfully pushed {rows_pushed} row(s) to Intake REDCap",
            "rows_pushed": rows_pushed,
        }

    except Exception as e:
        logger.exception("Error processing record %s for Intake REDCap", record_id)
        return {
            "status": "error",
            "record_id": record_id,
            "message": str(e),
        }


@app.get("/")
async def root() -> dict[str, str]:
    """Root endpoint."""
    return {"message": "REDCap to REDCap Migration Service"}


@app.get("/health")
async def health() -> dict[str, str]:
    """Health check endpoint."""
    return {"status": "healthy"}


@app.post("/webhook/redcap-to-intake")
async def redcap_to_intake_webhook(  # noqa: PLR0913
    background_tasks: BackgroundTasks,
    project_id: Annotated[int, Form()],
    instrument: Annotated[str, Form()],
    record: Annotated[str, Form()],
    redcap_event_name: Annotated[str | None, Form()] = None,
    redcap_repeat_instance: Annotated[int | None, Form()] = None,
    redcap_repeat_instrument: Annotated[str | None, Form()] = None,
    redcap_data_access_group: Annotated[str | None, Form()] = None,
    redcap_url: Annotated[str | None, Form()] = None,
    project_url: Annotated[str | None, Form()] = None,
    username: Annotated[str | None, Form()] = None,
    ready_to_send_to_intake_redcap: Annotated[str | None, Form()] = None,
) -> dict[str, Any]:
    """
    Handle REDCap Data Entry Trigger for Intake updates.

    This endpoint should be configured in REDCap as a Data Entry Trigger.
    When the 'ready_to_send_to_intake_redcap' field is set to '1', REDCap
    will POST to this endpoint.

    Configuration in REDCap:
    1. Go to Project Setup -> Additional Customizations
    2. Enable "Data Entry Trigger"
    3. Set URL to: https://your-domain.com/webhook/redcap-to-intake

    """
    logger.info(
        "Received REDCap trigger for record %s (instrument: %s)",
        record,
        instrument,
    )

    # Check if the ready flag is set
    if ready_to_send_to_intake_redcap != "1":
        logger.debug("Ready flag not set for record %s, ignoring trigger", record)
        return {
            "status": "ignored",
            "message": "Ready flag not set to '1', ignoring trigger",
            "record_id": record,
        }

    # Process the push in the background
    background_tasks.add_task(process_record_for_intake, record)

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


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8001)
