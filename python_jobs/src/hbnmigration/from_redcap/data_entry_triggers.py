"""Endpoint for catching REDCap data access triggers and launching relevant jobs."""

from typing import Any

from fastapi import BackgroundTasks, Depends, FastAPI, Request
from fastapi.responses import JSONResponse
import uvicorn

from ..utility_functions import (
    initialize_logging,
    safe_record_for_log,
)
from . import to_curious
from .from_redcap import RedcapRecord

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
async def redcap_data_access_trigger(
    background_tasks: BackgroundTasks, data: RedcapRecord = Depends()
) -> dict[str, Any]:
    """
    Handle REDCap Data Entry Trigger.

    Parameters
    ----------
    background_tasks
        Background tasks to be run after returning a response.
    data
        The data access trigger payload.
        See :class:`RedcapRecord` for detailed field descriptions.

    """
    safe_record = safe_record_for_log(data.record)
    logger.info(
        "Received REDCap trigger for record %s (instrument: %s)",
        safe_record,
        safe_record_for_log(data.instrument),
    )
    match data.project_id:
        case 625:
            if data.instrument == "enrollment_internal_use_only":
                background_tasks.add_task(to_curious.main)
    return {
        "status": "accepted",
        "message": f"Trigger accepted for instrument {data.instrument}",
        "project": data.project_id,
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
