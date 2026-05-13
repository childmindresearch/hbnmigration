"""Endpoint for catching REDCap data access triggers and launching relevant jobs."""

from typing import Annotated, Any

from fastapi import BackgroundTasks, FastAPI, Form, Request
from fastapi.responses import JSONResponse
import uvicorn

from ..utility_functions import (
    initialize_logging,
    safe_record_for_log,
)
from . import to_redcap

logger = initialize_logging(__name__)
"""Logger for data entry triggers."""
app = FastAPI(
    title="REDCap Data Entry Triggers Service",
    description="Handles REDCap Data Entry Triggers for Healthy Brain Network Data",
)
"""REDCap Data Entry Triggers Service."""

# ---------------------------------------------------------------------------
# Webhook endpoints
# ---------------------------------------------------------------------------


@app.get("/")
async def root() -> dict[str, str]:
    """Root endpoint."""
    return {"message": "REDCap to Curious Migration Service"}


@app.get("/health")
async def health() -> dict[str, str]:
    """Health check endpoint."""
    return {"status": "healthy"}


@app.post("/redcap-data-access-trigger")
async def redcap_data_access_trigger(  # noqa: PLR0913
    background_tasks: BackgroundTasks,
    project_id: Annotated[int, Form()],
    username: Annotated[str | None, Form()],
    instrument: Annotated[str, Form()],
    record: Annotated[str, Form()],
    redcap_event_name: Annotated[str | None, Form()] = None,
    redcap_data_access_group: Annotated[str | None, Form()] = None,
    instrument_complete: Annotated[int | None, Form()] = None,
    redcap_repeat_instance: Annotated[int | None, Form()] = None,
    redcap_repeat_instrument: Annotated[str | None, Form()] = None,
    redcap_url: Annotated[str | None, Form()] = None,
    project_url: Annotated[str | None, Form()] = None,
) -> dict[str, Any]:
    """
    Handle REDCap Data Entry Trigger.

    Parameters
    ----------
    background_tasks
        Background tasks to be run after returning a response.
    project_id
        The unique ID number of the REDCap project
        (i.e. the 'pid' value found in the URL when accessing the project in REDCap).
    username
        The username of the REDCap user that is triggering the Data Entry Trigger.
        Note: If it is triggered by a survey page (as opposed to a data entry form),
        then the username that will be reported will be '[survey respondent]'.
    instrument
        The unique name of the current data collection instrument
        (all your project's unique instrument names can be found in column B in the
        data dictionary).
    record
        The name of the record being created or modified,
        which is the record's value for the project's first field.
    redcap_event_name
        The unique event name of the event for which the record was modified
        (for longitudinal projects only).
    redcap_data_access_group
        The unique group name of the Data Access Group to which the record belongs
        (if the record belongs to a group).
    instrument_complete
        The status of the record for this particular data collection instrument,
        in which the value will be 0, 1, or 2.
        For data entry forms, 0=Incomplete, 1=Unverified, 2=Complete.
        For surveys, 0=partial survey response and 2=completed survey response.
        This parameter's name will be the variable name of this particular instrument's
        status field, which is the name of the instrument + '_complete'.
    redcap_repeat_instance
        The repeat instance number of the current instance of a repeating event OR
        repeating instrument.
        Note: This parameter is only sent in the request if the project contains
        repeating events/instruments *and* is currently saving a repeating
        event/instrument.
    redcap_repeat_instrument
        The unique instrument name of the current repeating instrument being saved.
        Note: This parameter is only sent in the request if the project contains
        repeating instruments *and* is currently saving a repeating instrument.
        Also, this parameter will not be sent for repeating events
        (as opposed to repeating instruments).
    redcap_url
        The base web address to REDCap (URL of REDCap's home page).
    project_url
        The base web address to the current REDCap project
        (URL of its Project Home page).

    """
    safe_record = safe_record_for_log(record)
    logger.info(
        "Received REDCap trigger for record %s (instrument: %s)",
        safe_record,
        safe_record_for_log(instrument),
    )
    match project_id:
        case 625:
            if instrument == "enrollment_internal_use_only":
                to_redcap.main()
    return {
        "status": "accepted",
        "message": f"Trigger accepted for record {record}",
        "record_id": record,
    }


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """Global exception handler."""
    logger.exception("Unhandled exception in REDCap to Curious service")
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error"},
    )


def serve(host: str = "0.0.0.0", port: int = 8002) -> None:
    """
    Start the webhook server.

    Args:
        host: Bind address.
        port: Bind port.

    """
    uvicorn.run(app, host=host, port=port)
