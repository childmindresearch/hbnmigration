"""
Transfer data from one REDCap project to another via webhook triggers.

When ``intake_ready`` field is set to 1 in
Healthy Brain Network Study Consent (IRB Approved) (PID 247),
copies the approved participants to HBN - Operations and Data Collection (PID 625) and
HBN - Curious outputs (PID 891).

Can also be run manually via CLI to process all pending records::

    python -m hbnmigration.from_redcap.to_redcap
"""

from datetime import date
from typing import Annotated, Any, cast

from fastapi import BackgroundTasks, FastAPI, Form, Request
from fastapi.responses import JSONResponse
import pandas as pd
from pydantic import Field
import uvicorn

from .._config_variables import redcap_variables
from ..exceptions import NoData
from ..utility_functions import initialize_logging, redcap_api_push, safe_record_for_log
from .config import Constraints, Fields, Values
from .from_redcap import fetch_data, RedcapRecord

logger = initialize_logging(__name__)
_REDCAP_TOKENS = redcap_variables.Tokens()
_REDCAP_ENDPOINTS = redcap_variables.Endpoints()
_SOURCE_PID = 247
_TARGET_PIDS = [625, 891]
_TARGET_PID_STRS = [str(_) for _ in _TARGET_PIDS]

app = FastAPI(
    title="REDCap to REDCap Migration Service",
    description=(
        "Handles REDCap Data Entry Triggers for pushing data "
        "from Intake (PID 247) to Operations (PID 625)"
    ),
)


class RedcapTriggerPayload(RedcapRecord):
    """Payload from REDCap Data Entry Trigger."""

    intake_ready: str | None = Field(default=None, alias="intake_ready")


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


def _compute_age(dob_str: str) -> int | None:
    """
    Compute age in years from a date-of-birth string.

    Parameters
    ----------
    dob_str
        Date of birth in ``YYYY-MM-DD`` format.

    Returns
    -------
    Age in whole years, or ``None`` if the string cannot be parsed.

    """
    try:
        dob = date.fromisoformat(str(dob_str).strip())
    except ValueError, TypeError:
        return None
    today = date.today()
    return today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))


def _apply_permission_audiovideo_age_rule(df: pd.DataFrame) -> pd.DataFrame:
    """
    Set ``permission_audiovideo_participant`` based on participant age.

    * Age < 11 **or** age ≥ 18 → ``"0"`` ("Not Applicable: no assent required")
    * Otherwise the value already present (from the
      ``permission_audiovideo_1113`` / ``permission_audiovideo_1417`` rename)
      is kept.

    If a record has no parseable DOB, the existing value is left unchanged.

    Parameters
    ----------
    df
        Long-format DataFrame with columns ``[record, field_name, value, ...]``.
        Field renaming must have already been applied so that DOB appears as
        ``"dob"`` and the audio/video field appears as
        ``"permission_audiovideo_participant"``.

    Returns
    -------
    DataFrame with adjusted ``permission_audiovideo_participant`` values.

    """
    not_applicable = str(
        Values.PID625.permission_audiovideo_participant[
            "Not Applicable: no assent required"
        ]
    )

    # Build a record → age mapping from the (already-renamed) dob field.
    dob_rows = df.loc[df["field_name"] == "dob", ["record", "value"]]
    if dob_rows.empty:
        return df

    record_age: dict[Any, int | None] = {
        row["record"]: _compute_age(row["value"]) for _, row in dob_rows.iterrows()
    }

    age_constraint = Constraints.PID625.permission_audiovideo_participant.age
    records_not_applicable = {
        rec
        for rec, age in record_age.items()
        if age is not None and not age_constraint.in_range(age)
    }

    if not records_not_applicable:
        return df

    perm_field = "permission_audiovideo_participant"

    # Update existing rows for these records.
    mask = (df["field_name"] == perm_field) & (
        df["record"].isin(records_not_applicable)
    )
    df.loc[mask, "value"] = not_applicable

    # Append rows for records that don't yet have the field.
    existing_records = set(df.loc[df["field_name"] == perm_field, "record"])
    missing_records = records_not_applicable - existing_records
    if missing_records:
        new_rows = pd.DataFrame(
            {
                "record": list(missing_records),
                "field_name": perm_field,
                "value": not_applicable,
            }
        )
        df = pd.concat([df, new_rows], ignore_index=True)

    return df


def format_data_for_redcap_operations(redcap_data: pd.DataFrame) -> pd.DataFrame:
    """
    Format data from intake (PID 247) for operations (PID 625).

    Applies field renaming, guardian consent mapping, record ID remapping,
    and other transformations needed for the target project.

    Parameters
    ----------
    redcap_data
        DataFrame with columns [record, field_name, value, redcap_event_name]

    Returns
    -------
    Formatted DataFrame ready for import to Operations REDCap.

    """
    df = redcap_data.copy()

    # Step 1: Apply field name transformations
    if hasattr(Fields.rename, "redcap_consent_to_redcap_operations"):
        df["field_name"] = df["field_name"].replace(
            Fields.rename.redcap_consent_to_redcap_operations
        )

    # Step 2: Update complete_parent_second_guardian_consent based on guardian2_consent
    df = update_complete_parent_second_guardian_consent(df)

    # Step 3: Apply age-based permission_audiovideo_participant rule
    df = _apply_permission_audiovideo_age_rule(df)

    # Step 4: Filter to only fields needed for operations project
    if hasattr(Fields, "import_625"):
        df = df[df["field_name"].str.startswith(tuple(Fields.import_625))]

    # Step 5: Build record ID mapping using MRN
    record_ids: dict[int | str, int | str] = {
        row["record"]: row["value"]
        for _, row in df[df["field_name"] == "mrn"].iterrows()
    }

    # Step 6: Remap record IDs
    df["record"] = df["record"].replace(record_ids)

    # Step 7: Update record_id field values to match new record numbers
    df.loc[df["field_name"] == "record_id", "value"] = df.loc[
        df["field_name"] == "record_id", "record"
    ]

    # Step 8: For repeated instruments,
    # keep only the latest instance's full set of rows.
    # Then deduplicate non-repeated rows by record + field_name.

    has_instance = df["redcap_repeat_instance"].notna()
    repeated_df = df[has_instance].copy()
    non_repeated_df = df[~has_instance].copy()

    if not repeated_df.empty:
        # Keep only rows from the highest instance per record + instrument
        max_instance = repeated_df.groupby(["record", "redcap_repeat_instrument"])[
            "redcap_repeat_instance"
        ].transform("max")
        repeated_df = repeated_df[repeated_df["redcap_repeat_instance"] == max_instance]

    # For non-repeated rows, deduplicate on record + field_name
    non_repeated_df = non_repeated_df.drop_duplicates(
        subset=["record", "field_name"], keep="first"
    )

    df = (
        pd.concat([non_repeated_df, repeated_df], ignore_index=True)
        .drop(
            columns=["redcap_repeat_instrument", "redcap_repeat_instance"],
            errors="ignore",
        )
        .reset_index(drop=True)
    )
    # Step 9: Decrement permission_collab values by 1
    decrement_mask = df["field_name"] == "permission_collab"
    if decrement_mask.any():
        decremented = (
            pd.to_numeric(df.loc[decrement_mask, "value"], errors="coerce") - 1
        )
        df.loc[decrement_mask, "value"] = decremented.astype(str)

    return df


def update_complete_parent_second_guardian_consent(df: pd.DataFrame) -> pd.DataFrame:
    """
    Update ``complete_parent_second_guardian_consent`` based on ``guardian2_consent``.

    Only records whose ``guardian2_consent`` value is in the mapping are affected.
    All other records are left unchanged.
    """
    mapping = {
        Values.PID247.guardian2_consent[
            _consent
        ]: Values.PID625.complete_parent_second_guardian_consent[_operations]
        for _consent, _operations in [
            ("No", "Not Required"),
            (
                "Not Applicable (Adult Participant)",
                "Not Applicable (Adult Participant)",
            ),
        ]
    }

    # Compute desired target value per record
    record_to_value = (
        df.query("field_name == 'guardian2_consent'")
        .set_index("record")["value"]
        .map(mapping)
        .dropna()
    )
    if record_to_value.empty:
        return df

    records_to_update = record_to_value.index

    # Update existing rows
    mask = (df["field_name"] == "complete_parent_second_guardian_consent") & (
        df["record"].isin(records_to_update)
    )
    df.loc[mask, "value"] = df.loc[mask, "record"].map(record_to_value)

    # Append missing rows
    missing_records = records_to_update.difference(
        df.loc[
            df["field_name"] == "complete_parent_second_guardian_consent", "record"
        ].tolist()
    )
    if len(missing_records):
        df = pd.concat(
            [
                df,
                pd.DataFrame(
                    {
                        "record": missing_records,
                        "field_name": "complete_parent_second_guardian_consent",
                        "value": record_to_value.loc[missing_records].values,
                    }
                ),
            ],
            ignore_index=True,
        )

    return df.sort_values(["record", "field_name"], kind="stable").reset_index(
        drop=True
    )


def push_to_intake_redcap(source_data: pd.DataFrame) -> int:
    """
    Push data to operations (PID 625) and Curious data (891).

    Args:
        source_data: DataFrame with formatted data to push.

    Returns:
        Number of rows successfully pushed.

    """
    rows_updated = []
    for project in _TARGET_PIDS:
        try:
            rows_updated.append(
                redcap_api_push(
                    df=source_data,
                    token=getattr(_REDCAP_TOKENS, f"pid{project}"),
                    url=_REDCAP_ENDPOINTS.base_url,
                    headers=redcap_variables.headers,
                )
            )
            logger.info(
                "%d rows successfully pushed to Operations REDCap (PID %d).",
                rows_updated,
                project,
            )
        except Exception:
            logger.exception(
                "Failed to push data to Operations REDCap (PID %d)", project
            )
            raise
    assert rows_updated[0] == rows_updated[1]
    return rows_updated[0]


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
                    "field_name": "intake_ready",
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
            {"fields": "intake_ready"},
            filter_logic=f"[record_id] = '{record_id}'",
        )
        if data.empty:
            logger.warning(
                "Could not find record %s in PID %d to clear flag",
                record_id,
                _SOURCE_PID,
            )
            return

        event = event_map(data).get("intake_ready")
        if event is None:
            logger.error(
                "Could not determine redcap_event_name for "
                "'intake_ready'. "
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
            # Make sure to include intake_ready in the export
            fields_to_export = str(Fields.export_247.for_redcap_operations)
            if "intake_ready" not in fields_to_export:
                fields_to_export += ",intake_ready"
        else:
            fields_to_export = "intake_ready"

        source_data = fetch_data(
            _REDCAP_TOKENS.pid247,
            {"fields": fields_to_export},
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
        # Extract event name for intake_ready field BEFORE formatting
        event_name = None
        intake_ready_rows = source_data[source_data["field_name"] == "intake_ready"]
        if (
            not intake_ready_rows.empty
            and "redcap_event_name" in intake_ready_rows.columns
        ):
            event_name = intake_ready_rows["redcap_event_name"].iloc[0]

        formatted_data = format_data_for_redcap_operations(source_data)

        if formatted_data.empty:
            logger.info("No processable data for record %s", record_id)
            return {
                "status": "error",
                "record_id": record_id,
                "message": "No processable data after formatting",
            }

        rows_pushed = push_to_intake_redcap(formatted_data)

        # Update the flag with the captured event name
        if event_name:
            update_source_redcap_status(
                record_id,
                str(
                    Values.PID247.intake_ready[
                        "Participant information already sent to "
                        "HBN - Intake Redcap project"
                    ]
                ),
                event_name,
            )
        else:
            logger.warning(
                "Could not determine event name for record %s, skipping status update",
                record_id,
            )

        return {
            "status": "success",
            "record_id": record_id,
            "message": f"Pushed {rows_pushed} row(s) to Operations (PIDS "
            f"{{', '.join(_TARGET_PID_STRS}}",
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
    intake_ready: Annotated[str | None, Form()] = None,
) -> dict[str, Any]:
    """
    Handle REDCap Data Entry Trigger for Intake updates.

    This endpoint should be configured as a Data Entry Trigger in
    REDCap PID 247. When ``intake_ready`` is set to ``1``,
    REDCap will POST to this endpoint.

    Configuration in REDCap:
    1. Go to Project Setup → Additional Customizations
    2. Enable "Data Entry Trigger"
    3. Set URL to: ``https://your-domain.com/webhook/redcap-to-intake``
    """
    safe_record = safe_record_for_log(record)
    logger.info(
        "Received REDCap trigger for record %s (instrument: %s)",
        safe_record,
        safe_record_for_log(instrument),
    )

    if intake_ready != "1":
        logger.debug("Ready flag not set for record %s, ignoring trigger", safe_record)
        return {
            "status": "ignored",
            "message": "Ready flag not set to '1', ignoring trigger",
            "record_id": record,
        }

    background_tasks.add_task(process_record_for_redcap_operations, record)
    logger.info("Queued record %s for Intake REDCap push", safe_record)
    return {
        "status": "accepted",
        "message": f"Trigger accepted for record {safe_record}",
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
            {"fields": fields_to_export} if fields_to_export else {},
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
            " | ".join(_TARGET_PID_STRS),
        )
        return
    # Build mapping of MRN -> source record_id BEFORE formatting
    mrn_to_source_record = (
        data_operations[data_operations["field_name"] == "mrn"]
        .set_index("value")["record"]
        .to_dict()
    )

    # Also capture event names for each source record
    source_record_to_event = {}
    intake_ready_rows = data_operations[data_operations["field_name"] == "intake_ready"]
    if not intake_ready_rows.empty and "redcap_event_name" in intake_ready_rows.columns:
        source_record_to_event = intake_ready_rows.set_index("record")[
            "redcap_event_name"
        ].to_dict()

    formatted_data = format_data_for_redcap_operations(data_operations)

    if formatted_data.empty:
        logger.info("No data to push after formatting.")
        return

    try:
        push_to_intake_redcap(formatted_data)
    except Exception:
        logger.exception("Batch push to Intake REDCap failed.")
        return

    # Get the MRNs (which are now in the 'record' column after remapping)
    pushed_mrns = set(formatted_data["record"].unique())

    # Track successful updates
    successful_updates = 0

    # Map back to original source record IDs and update their status
    for mrn in pushed_mrns:
        source_record_id = mrn_to_source_record.get(mrn)
        if source_record_id:
            event_name = source_record_to_event.get(source_record_id, "")
            try:
                update_source_redcap_status(
                    str(source_record_id),
                    str(
                        Values.PID247.intake_ready[
                            "Participant information already sent to "
                            "HBN - Intake Redcap project"
                        ]
                    ),
                    event_name,
                )
                successful_updates += 1
            except Exception:
                logger.exception(
                    "Failed to update status for source record %s (MRN: %s)",
                    source_record_id,
                    mrn,
                )

    # Verify consistency
    if successful_updates != len(pushed_mrns):
        logger.warning(
            "Mismatch: pushed %d records but only updated %d source records",
            len(pushed_mrns),
            successful_updates,
        )


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
