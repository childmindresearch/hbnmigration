"""
Transfer data from REDCap to Curious via webhook triggers.

When `ready_to_send_to_curious` field is set to 1 in REDCap,
prepares and copies the reviewed and approved participants by the RAs to Curious.

Can also be run manually via CLI to process all pending records:
    python -m hbnmigration.from_redcap.to_curious
"""

from typing import Annotated, Any, cast, Literal

from fastapi import BackgroundTasks, FastAPI, Form, Request
from fastapi.responses import JSONResponse
import numpy as np
import pandas as pd
from pydantic import BaseModel, Field
import requests
import uvicorn

from .._config_variables import curious_variables, redcap_variables
from ..exceptions import NoData
from ..from_curious.config import AccountType
from ..utility_functions import (
    initialize_logging,
    new_curious_account,
    redcap_api_push,
)
from .config import Fields, Values
from .from_redcap import fetch_data

logger = initialize_logging(__name__)

Individual = Literal["child", "parent"]
INDIVIDUALS: list[Individual] = ["parent", "child"]
_REDCAP_TOKENS = redcap_variables.Tokens()
_REDCAP_PID = 625

app = FastAPI(
    title="REDCap to Curious Migration Service",
    description="Handles REDCap Data Entry Triggers for pushing data to Curious",
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
    ready_to_send_to_curious: str | None = Field(None, alias="ready_to_send_to_curious")

    class Config:
        """Pydantic config."""

        populate_by_name = True


def _in_set(x: set | int | str, required_value: int | str = 1) -> bool:
    """Check if required value in "parental_involvement" column."""
    if isinstance(x, (int, str)):
        x = {x}
    if not isinstance(x, (list, set)):
        return False
    return str(required_value) in [str(_) for _ in x]


def _check_for_data_to_process(df: pd.DataFrame, account_type: AccountType) -> None:
    """Check for data to process and log result."""
    if df.loc[df["accountType"] == account_type].empty:
        logger.info("There is not %s consent data to process.", account_type)
    else:
        logger.info(
            "%s data was prepared to be sent to the Curious API.",
            account_type.capitalize(),
        )


def event_map(redcap_data: pd.DataFrame) -> dict[str, str]:
    """
    Build a mapping from `field_name` to `redcap_event_name`.

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


def _format_redcap_data_for_curious(
    redcap_data: pd.DataFrame, individual: Literal["child", "parent"]
) -> pd.DataFrame:
    """For a class of individual, format REDCap data for Curious."""
    record_set: set[int | str] = set()
    df_temp = pd.DataFrame(redcap_data[["record", "field_name", "value"]]).copy()
    df_temp["field_name"] = df_temp["field_name"].replace(
        getattr(Fields.rename.redcap_operations_to_curious, individual)
    )
    # Filter to relevant fields
    individual_fields: dict[str, int | str | None] = getattr(
        Fields.import_curious, individual
    )
    relevant_fields = list(individual_fields.keys())
    df_temp = df_temp[df_temp["field_name"].isin(relevant_fields)]
    df_temp = (
        df_temp.groupby(["record", "field_name"])["value"]
        .apply(lambda x: set(x) if len(x) > 1 else x.iloc[0])
        .reset_index()
    )
    # Pivot
    df_pivoted = df_temp.pivot(index="record", columns="field_name", values="value")
    record_set = {*record_set, *df_pivoted.index.tolist()}
    # Add missing columns with defaults
    for field, default_value in individual_fields.items():
        if field not in df_pivoted.columns:
            df_pivoted[field] = default_value
    df = df_pivoted.reset_index(drop=True)
    return df.where(pd.notna(df), np.nan)


def format_redcap_data_for_curious(
    redcap_data: pd.DataFrame,
) -> dict[Literal["child", "parent"], pd.DataFrame]:
    """Format REDCap export data for Curious import."""
    curious_participant_data: dict[Literal["child", "parent"], pd.DataFrame] = {
        individual: _format_redcap_data_for_curious(redcap_data, individual)
        for individual in INDIVIDUALS
    }
    if "parent_involvement" in curious_participant_data["child"].columns:
        curious_participant_data["child"] = pd.DataFrame(
            curious_participant_data["child"][
                curious_participant_data["child"]["parent_involvement"].apply(_in_set)
                | curious_participant_data["child"]["parent_involvement"].isna()
            ]
        ).dropna(axis=1, how="all")
    # Now drop `parent_involvement` column before we push to Curious.
    curious_participant_data["child"] = curious_participant_data["child"].drop(
        columns=["parent_involvement", "adult_enrollment_form_complete"],
        errors="ignore",
    )
    # Pad `secretUserId` with leading zeros to make it 5 characters long
    curious_participant_data["child"]["secretUserId"] = (
        curious_participant_data["child"]["secretUserId"].astype(str).str.zfill(5)
    )
    return curious_participant_data


def send_to_curious(
    df: pd.DataFrame,
    tokens: curious_variables.Tokens,
    applet_id: str,
) -> list[str]:
    """Send new participants to Curious (no caching)."""
    failures: list[str] = []
    headers = curious_variables.headers(tokens.access)
    # Loop through each REDCap transformed record and send it to MindLogger
    for record in [
        {k: v for k, v in record.items() if v is not None}
        for record in df.to_dict(orient="records")
    ]:
        secret_user_id = record.get("secretUserId", "")
        mrn = stringify_secret_user_id(secret_user_id) if secret_user_id else ""
        try:
            logger.info(
                "%s",
                new_curious_account(
                    tokens.endpoints.base_url, applet_id, record, headers
                ),
            )
        except requests.exceptions.RequestException:
            logger.exception("Error sending MRN %s to Curious", mrn)
            failures.append(mrn)
    return failures


def stringify_secret_user_id(secret_user_id: int | str) -> str:
    """Return string with leading zeroes dropped."""
    try:
        return str(int(secret_user_id))
    except TypeError, ValueError:
        return str(secret_user_id)


def update_redcap(
    redcap_df: pd.DataFrame, curious_df: pd.DataFrame, failures: list[str]
) -> None:
    """
    Update records in REDCap.

    Uses the field_name → redcap_event_name mapping from the original data so
    that the enrollment_complete update is written to the correct event in PID 625.
    """
    # Get updated records
    records = [stringify_secret_user_id(x) for x in curious_df["secretUserId"]]
    df_update_redcap = redcap_df.query(
        f'field_name == "mrn" and value in {records}'
    ).copy()[["record", "field_name", "value"]]
    # Set updated `enrollment_complete`
    df_update_redcap["field_name"] = "enrollment_complete"
    df_update_redcap["value"] = Values.PID625.enrollment_complete[
        "Parent and Participant information already sent to Curious"
    ]
    successes = set(
        redcap_df[
            (redcap_df["field_name"] == "mrn") & (~redcap_df["value"].isin(failures))
        ]["record"]
    )
    df_update_redcap = df_update_redcap[df_update_redcap["record"].isin(successes)]
    # Look up the correct event for enrollment_complete
    enrollment_event = event_map(redcap_df).get("enrollment_complete")
    if enrollment_event is None:
        logger.error(
            "Could not determine redcap_event_name for 'enrollment_complete'. "
            "Skipping REDCap update."
        )
        return
    df_update_redcap["redcap_event_name"] = enrollment_event
    if df_update_redcap.empty:
        logger.info("No REDCap records to update.")
        return
    try:
        rows_updated = redcap_api_push(
            df=df_update_redcap[["record", "redcap_event_name", "field_name", "value"]],
            token=_REDCAP_TOKENS.pid625,
            url=redcap_variables.Endpoints().base_url,
            headers=redcap_variables.headers,
        )
        logger.info(
            "%d rows successfully updated in PID %d.", rows_updated, _REDCAP_PID
        )
    except Exception:
        logger.exception("REDCap status update failed.")
        raise


def clear_ready_flag(record_id: str) -> None:
    """
    Clear the ready-to-send flag after successful push.

    Args:
        record_id: The record ID to update.

    """
    try:
        data = fetch_data(
            _REDCAP_TOKENS.pid625,
            "ready_to_send_to_curious",
            filter_logic=f"[record_id] = '{record_id}'",
        )
        if data.empty:
            logger.warning("Could not find record %s to clear flag", record_id)
            return
        event = event_map(data).get("ready_to_send_to_curious")
        if event is None:
            logger.error(
                "Could not determine redcap_event_name for "
                "'ready_to_send_to_curious'. "
                "Skipping flag clear for record %s.",
                record_id,
            )
            return
        update_data = pd.DataFrame(
            [
                {
                    "record": record_id,
                    "redcap_event_name": event,
                    "field_name": "ready_to_send_to_curious",
                    "value": "0",
                }
            ]
        )
        redcap_api_push(
            df=update_data,
            token=_REDCAP_TOKENS.pid625,
            url=redcap_variables.Endpoints().base_url,
            headers=redcap_variables.headers,
        )
        logger.info("Cleared ready flag for record %s", record_id)
    except Exception:
        logger.exception("Error clearing ready flag for record %s", record_id)


def push_child_data(
    child_data: pd.DataFrame,
    curious_endpoints: curious_variables.Endpoints,
    curious_credentials: curious_variables.AppletCredentials,
) -> list[str]:
    """
    Push child (full) data to the child Curious applet.

    Args:
        child_data: Child DataFrame with ``accountType`` already set.
        curious_endpoints: Curious API endpoints.
        curious_credentials: Curious applet credentials.

    Returns:
        List of MRNs that failed to send.

    """
    applet_name = "CHILD-Healthy Brain Network Questionnaires"
    curious_tokens = curious_variables.Tokens(
        curious_endpoints, curious_credentials[applet_name]
    )
    return send_to_curious(
        child_data,
        curious_tokens,
        curious_variables.applets[applet_name].applet_id,
    )


def push_parent_data(
    child_data_limited: pd.DataFrame,
    parent_data: pd.DataFrame,
    curious_endpoints: curious_variables.Endpoints,
    curious_credentials: curious_variables.AppletCredentials,
) -> list[str]:
    """
    Push parent (full) and child (limited) data to the parent Curious applet.

    Args:
        child_data_limited: Child DataFrame with ``accountType`` set to
            ``"limited"``.
        parent_data: Parent DataFrame with ``accountType`` set to ``"full"``.
        curious_endpoints: Curious API endpoints.
        curious_credentials: Curious applet credentials.

    Returns:
        List of MRNs that failed to send.

    """
    applet_name = "Healthy Brain Network Questionnaires"
    curious_tokens = curious_variables.Tokens(
        curious_endpoints, curious_credentials[applet_name]
    )
    applet_id = curious_variables.applets[applet_name].applet_id
    return [
        *send_to_curious(child_data_limited, curious_tokens, applet_id),
        *send_to_curious(parent_data, curious_tokens, applet_id),
    ]


def _prepare_curious_data(
    curious_data: dict[Literal["child", "parent"], pd.DataFrame],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Prepare DataFrames with ``accountType`` set for each destination.

    Args:
        curious_data: Formatted child and parent DataFrames from
            :func:`format_redcap_data_for_curious`.

    Returns:
        A tuple of (child_full, child_limited, parent_full).

    """
    child_full = curious_data["child"].copy()
    child_full["accountType"] = "full"

    child_limited = curious_data["child"].copy()
    child_limited["accountType"] = "limited"

    parent_full = curious_data["parent"].copy()
    parent_full["accountType"] = "full"

    return child_full, child_limited, parent_full


def _push_to_curious(
    data_operations: pd.DataFrame,
    curious_data: dict[Literal["child", "parent"], pd.DataFrame],
) -> list[str]:
    """
    Validate, push, and update REDCap for a batch of formatted records.

    Args:
        data_operations: Raw REDCap export data (used for the REDCap update).
        curious_data: Formatted child and parent DataFrames from
            :func:`format_redcap_data_for_curious`.

    Returns:
        List of MRNs that failed to send.

    """
    child_full, child_limited, parent_full = _prepare_curious_data(curious_data)
    _check_for_data_to_process(child_full, "full")
    _check_for_data_to_process(child_limited, "limited")
    _check_for_data_to_process(parent_full, "full")

    curious_endpoints = curious_variables.Endpoints()
    curious_credentials = curious_variables.AppletCredentials()
    failures = [
        *push_child_data(child_full, curious_endpoints, curious_credentials),
        *push_parent_data(
            child_limited, parent_full, curious_endpoints, curious_credentials
        ),
    ]
    update_redcap(data_operations, curious_data["child"], failures)
    return failures


def process_record_for_curious(record_id: str) -> dict[str, Any]:
    """
    Process a single record triggered by REDCap webhook.

    Args:
        record_id: The record ID from the trigger.

    Returns:
        Dictionary with status and message.

    """
    try:
        logger.info("Processing record %s for Curious push", record_id)
        data_operations = fetch_data(
            _REDCAP_TOKENS.pid625,
            str(Fields.export_operations.for_curious),
            filter_logic=f"[record_id] = '{record_id}'",
        )
        if data_operations.empty:
            logger.warning("No data found for record %s", record_id)
            return {
                "status": "error",
                "record_id": record_id,
                "message": "No data found in REDCap",
            }

        curious_data = format_redcap_data_for_curious(data_operations)
        if curious_data["child"].empty and curious_data["parent"].empty:
            logger.info("No processable data for record %s", record_id)
            return {
                "status": "error",
                "record_id": record_id,
                "message": "No processable data after formatting",
            }

        failures = _push_to_curious(data_operations, curious_data)
        clear_ready_flag(record_id)

        if failures:
            return {
                "status": "partial",
                "record_id": record_id,
                "message": f"Processed with {len(failures)} failure(s)",
                "failures": failures,
            }
        return {
            "status": "success",
            "record_id": record_id,
            "message": "Successfully pushed to Curious",
        }
    except Exception as e:
        logger.exception("Error processing record %s for Curious", record_id)
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
    return {"message": "REDCap to Curious Migration Service"}


@app.get("/health")
async def health() -> dict[str, str]:
    """Health check endpoint."""
    return {"status": "healthy"}


@app.post("/webhook/redcap-to-curious")
async def redcap_to_curious_webhook(
    background_tasks: BackgroundTasks,
    # project_id: Annotated[int, Form()],
    instrument: Annotated[str, Form()],
    record: Annotated[str, Form()],
    # redcap_event_name: Annotated[str | None, Form()] = None,
    # redcap_repeat_instance: Annotated[int | None, Form()] = None,
    # redcap_repeat_instrument: Annotated[str | None, Form()] = None,
    # redcap_data_access_group: Annotated[str | None, Form()] = None,
    # redcap_url: Annotated[str | None, Form()] = None,
    # project_url: Annotated[str | None, Form()] = None,
    # username: Annotated[str | None, Form()] = None,
    ready_to_send_to_curious: Annotated[str | None, Form()] = None,
) -> dict[str, Any]:
    """
    Handle REDCap Data Entry Trigger for Curious updates.

    This endpoint should be configured in REDCap as a Data Entry Trigger.
    When the ``ready_to_send_to_curious`` field is set to ``1``, REDCap
    will POST to this endpoint.

    Configuration in REDCap:

    1. Go to Project Setup → Additional Customizations
    2. Enable "Data Entry Trigger"
    3. Set URL to: ``https://your-domain.com/webhook/redcap-to-curious``

    """
    logger.info(
        "Received REDCap trigger for record %s (instrument: %s)",
        record,
        instrument,
    )
    if ready_to_send_to_curious != "1":
        logger.debug("Ready flag not set for record %s, ignoring trigger", record)
        return {
            "status": "ignored",
            "message": "Ready flag not set to '1', ignoring trigger",
            "record_id": record,
        }
    background_tasks.add_task(process_record_for_curious, record)
    logger.info("Queued record %s for Curious push", record)
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


# ---------------------------------------------------------------------------
# CLI entry points
# ---------------------------------------------------------------------------


def main() -> None:
    """
    Process all pending 'Ready to Send to Curious' records (bulk/manual).

    This is the original batch-processing entry point. Run manually or via
    cron to process every record currently flagged in REDCap PID 625.

    Usage::

        python -m hbnmigration.from_redcap.to_curious
    """
    try:
        data_operations = fetch_data(
            _REDCAP_TOKENS.pid625,
            str(Fields.export_operations.for_curious),
            Values.PID625.enrollment_complete.filter_logic("Ready to Send to Curious"),
        )
        if data_operations.empty:
            logger.info(
                "REDCap PID %s: No participants marked 'Ready to Send to Curious'.",
                _REDCAP_PID,
            )
            raise NoData
    except NoData:
        logger.info("No data to transfer from REDCap %s to Curious.", _REDCAP_PID)
        return

    curious_data = format_redcap_data_for_curious(data_operations)
    if curious_data["child"].empty and curious_data["parent"].empty:
        logger.info("All participants already sent to Curious")
        return

    _push_to_curious(data_operations, curious_data)


def serve(host: str = "0.0.0.0", port: int = 8002) -> None:
    """
    Start the webhook server.

    Args:
        host: Bind address.
        port: Bind port.

    """
    uvicorn.run(app, host=host, port=port)


if __name__ == "__main__":
    main()
