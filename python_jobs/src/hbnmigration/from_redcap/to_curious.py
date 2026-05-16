"""
Transfer data from REDCap to Curious via webhook triggers.

When `ready_to_send_to_curious` field is set to 1 in REDCap,
prepares and copies the reviewed and approved participants by the RAs to Curious.

Can also be run manually via CLI to process all pending records:
    python -m hbnmigration.from_redcap.to_curious
"""

from __future__ import annotations

from typing import Any, cast, Literal

import numpy as np
import pandas as pd
import requests

from .._config_variables import curious_variables, redcap_variables
from ..exceptions import NoData
from ..from_curious.config import AccountType
from ..utility_functions import initialize_logging, new_curious_account, redcap_api_push
from .config import Fields, Values
from .from_redcap import fetch_data

logger = initialize_logging(__name__)

Individual = Literal["child", "parent"]
INDIVIDUALS: list[Individual] = ["parent", "child"]
_REDCAP_TOKENS = redcap_variables.Tokens()
_REDCAP_PID = 625

# Slot fields in PID 625 and their corresponding "ready/sent" status fields.
_SLOT_TO_STATUS_FIELD: dict[str, str] = {
    "r_id": "enrollment_complete",
    "r_id_2": "enrollment_complete_2",
    "r_id_3": "enrollment_complete_3",
}


def _failure_sets(failures: list[str]) -> tuple[set[str], set[str]]:
    """
    Build failure sets for different matching needs.

    Args:
        failures: Raw secretUserId values that failed to send to Curious.

    Returns:
        (raw_failures, mrn_like_failures)

    Notes:
        - raw_failures are used for responder-slot filtering (exact match).
        - mrn_like_failures are used for participant MRN matching in REDCap and
          are derived only from *numeric* IDs (or 'r'+numeric).
          We intentionally do NOT treat 'R000123' (common responder IDs) as MRNs.

    """
    raw = {str(x) for x in failures}

    mrn_like: set[str] = set()
    for x in raw:
        s = x.strip()

        # Treat plain numeric as MRN-like
        if s.isdigit():
            mrn_like.add(stringify_secret_user_id(s))
            continue

        # Treat lowercase 'r' + numeric as MRN-like (parent id convention)
        if s.startswith("r") and s[1:].isdigit():
            mrn_like.add(stringify_secret_user_id(s[1:]))
            continue

        # Anything else is not MRN-like (e.g., 'R000123'); ignore for MRN failures.

    return raw, mrn_like


def _extract_ready_responder_slots(data_operations: pd.DataFrame) -> pd.DataFrame:
    """
    Extract ready responder slots (r_id*) with context.

    This avoids pandas pivot() failures on EAV exports that contain duplicate
    (record, field_name) rows across events or repeat instances.

    Args:
        data_operations: PID 625 EAV export data.

    Returns:
        DataFrame with columns:
            - record: PID 625 record id (string)
            - redcap_event_name: event where the ready flag was observed (string)
            - secretUserId: responder record_id in PID 879 (string)
            - status_field: which enrollment_complete* field corresponds to this slot

    """
    id_fields = list(_SLOT_TO_STATUS_FIELD.keys())
    status_fields = list(_SLOT_TO_STATUS_FIELD.values())
    relevant_fields = {*id_fields, *status_fields}

    subset = data_operations[data_operations["field_name"].isin(relevant_fields)].copy()
    if subset.empty:
        return pd.DataFrame(
            columns=["record", "redcap_event_name", "secretUserId", "status_field"]
        )

    # Pivot at a grain that is unique in longitudinal/repeating projects.
    index_cols: list[str] = ["record"]
    for col in (
        "redcap_event_name",
        "redcap_repeat_instrument",
        "redcap_repeat_instance",
    ):
        if col in subset.columns:
            index_cols.append(col)

    # pivot_table tolerates duplicates; pivot does not.
    wide = subset.pivot_table(
        index=index_cols,
        columns="field_name",
        values="value",
        aggfunc="first",
    ).reset_index()

    ready_value = Values.PID625.enrollment_complete["Ready to Send to Curious"]
    ready_value_str = str(ready_value)

    rows: list[dict[str, str]] = []
    for id_field, status_field in _SLOT_TO_STATUS_FIELD.items():
        if id_field not in wide.columns or status_field not in wide.columns:
            continue

        mask = wide[status_field].astype(str) == ready_value_str
        if not mask.any():
            continue

        # Same event + same instrument: take responder ID from same row.
        cols = ["record", id_field]
        if "redcap_event_name" in wide.columns:
            cols.insert(1, "redcap_event_name")

        ready_rows = wide.loc[mask, cols].dropna(subset=[id_field])

        for _, r in ready_rows.iterrows():
            rows.append(
                {
                    "record": str(r["record"]),
                    "redcap_event_name": str(r.get("redcap_event_name", "")),
                    "secretUserId": str(r[id_field]),
                    "status_field": status_field,
                }
            )

    out = pd.DataFrame(rows)
    if out.empty:
        return pd.DataFrame(
            columns=["record", "redcap_event_name", "secretUserId", "status_field"]
        )

    out["record"] = out["record"].astype(str)
    out["secretUserId"] = out["secretUserId"].astype(str)
    out["redcap_event_name"] = out["redcap_event_name"].astype(str)
    return out


def _dedupe_by_best_row(df: pd.DataFrame, key: str) -> pd.DataFrame:
    """
    Deduplicate by keeping the "best" row per key.

    "Best" is defined as:
        1) has email (strongly preferred)
        2) has the most non-null fields overall

    Args:
        df: Input DataFrame.
        key: Column name to deduplicate on.

    Returns:
        Deduplicated DataFrame.

    """
    if df.empty or key not in df.columns:
        return df

    tmp = df.copy()
    email_present = tmp["email"].notna().astype(int) if "email" in tmp.columns else 0
    tmp["_score"] = tmp.notna().sum(axis=1) + (email_present * 100)

    return (
        tmp.sort_values("_score", ascending=False)
        .drop_duplicates(subset=[key], keep="first")
        .drop(columns=["_score"])
    )


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


def _fetch_responder_emails() -> pd.DataFrame:
    """
    Fetch responder emails from PID 879 (record_id -> resp_email).

    Returns:
        DataFrame with columns:
            - secretUserId: PID 879 record_id (string)
            - email: responder email

    """
    data = fetch_data(
        _REDCAP_TOKENS.pid879,
        {"fields": "record_id,resp_email"},
    )
    if data.empty:
        return pd.DataFrame(columns=["secretUserId", "email"])

    # EAV -> wide (PID 879 is not expected to be longitudinal here,
    # but pivot_table is safe)
    wide = (
        data[data["field_name"].isin(["record_id", "resp_email"])]
        .pivot_table(
            index="record", columns="field_name", values="value", aggfunc="first"
        )
        .reset_index(drop=True)
        .rename(columns={"record_id": "secretUserId", "resp_email": "email"})
    )

    wide["secretUserId"] = wide["secretUserId"].astype(str)
    return wide[["secretUserId", "email"]]


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
    df_temp = pd.DataFrame(redcap_data[["record", "field_name", "value"]]).copy()
    df_temp["field_name"] = df_temp["field_name"].replace(
        getattr(Fields.rename.redcap_operations_to_curious, individual)
    )

    individual_fields: dict[str, int | str | None] = getattr(
        Fields.import_curious, individual
    )
    relevant_fields = list(individual_fields.keys())
    df_temp = df_temp[df_temp["field_name"].isin(relevant_fields)]
    df_temp = df_temp.groupby(["record", "field_name"])["value"].first().reset_index()

    df_pivoted = df_temp.pivot(index="record", columns="field_name", values="value")

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

    # Pad `secretUserId` with leading zeros to make it 5 characters long for child IDs
    if "secretUserId" in curious_participant_data["child"].columns:
        curious_participant_data["child"]["secretUserId"] = (
            curious_participant_data["child"]["secretUserId"].astype(str).str.zfill(5)
        )

    return curious_participant_data


def send_to_curious(
    df: pd.DataFrame,
    tokens: curious_variables.Tokens,
    applet_id: str,
) -> list[str]:
    """
    Send new participants to Curious (no caching).

    Important:
        Returns failures as the *raw secretUserId* that was sent.

    """
    failures: list[str] = []
    headers = curious_variables.headers(tokens.access)

    for record in [
        {k: v for k, v in record.items() if v is not None}
        for record in df.to_dict(orient="records")
    ]:
        secret_user_id_raw = str(record.get("secretUserId", "") or "")
        mrn_for_log = (
            stringify_secret_user_id(secret_user_id_raw) if secret_user_id_raw else ""
        )

        try:
            logger.info(
                "%s",
                new_curious_account(
                    tokens.endpoints.base_url, applet_id, record, headers
                ),
            )
        except requests.exceptions.RequestException:
            logger.exception(
                "Error sending secretUserId=%s (mrn-like=%s) to Curious",
                secret_user_id_raw,
                mrn_for_log,
            )
            if secret_user_id_raw:
                failures.append(secret_user_id_raw)

    return failures


def stringify_secret_user_id(secret_user_id: int | str) -> str:
    """Return string with leading zeroes dropped (when numeric)."""
    try:
        return str(int(secret_user_id))
    except TypeError, ValueError:
        return str(secret_user_id)


def update_redcap(
    redcap_df: pd.DataFrame,
    curious_child_df: pd.DataFrame,
    failures: list[str],
    responder_slots_ready: pd.DataFrame,
) -> None:
    """
    Update records in REDCap (PID 625).

    Updates:
      1) Participant-level status using MRN match (existing behavior)
      2) Responder slot statuses (enrollment_complete, _2, _3) for successful slots

    Notes:
        - failures are *raw* secretUserIds (exactly as sent).
        - responder slots are filtered using exact raw failures.
        - participant MRN failures are derived only from numeric / 'r'+numeric.

    """
    raw_failures, mrn_like_failures = _failure_sets(failures)

    updates: list[pd.DataFrame] = []

    sent_value = Values.PID625.enrollment_complete[
        "Parent and Participant information already sent to Curious"
    ]

    # -------------------------
    # (A) Participant update (existing behavior)
    # -------------------------
    if "secretUserId" in curious_child_df.columns and not curious_child_df.empty:
        # REDCap mrn values are typically unpadded numeric strings;
        # normalize child IDs to that form.
        mrn_records = [
            stringify_secret_user_id(x) for x in curious_child_df["secretUserId"]
        ]

        df_mrn_rows = redcap_df.loc[
            (redcap_df["field_name"] == "mrn")
            & (redcap_df["value"].astype(str).isin(mrn_records)),
            ["record", "field_name", "value"],
        ].copy()

        if not df_mrn_rows.empty:
            df_participant_update = df_mrn_rows.copy()
            df_participant_update["field_name"] = "enrollment_complete"
            df_participant_update["value"] = sent_value

            # Exclude records whose MRN failed (mrn-like failures only)
            successes = set(
                redcap_df.loc[
                    (redcap_df["field_name"] == "mrn")
                    & (~redcap_df["value"].astype(str).isin(mrn_like_failures)),
                    "record",
                ].astype(str)
            )
            df_participant_update["record"] = df_participant_update["record"].astype(
                str
            )
            df_participant_update = df_participant_update[
                df_participant_update["record"].isin(successes)
            ]

            # Determine correct event for enrollment_complete (legacy behavior)
            enrollment_event = event_map(redcap_df).get("enrollment_complete")
            if enrollment_event is None:
                logger.error(
                    "Could not determine redcap_event_name for 'enrollment_complete'. "
                    "Skipping participant REDCap update."
                )
            else:
                df_participant_update["redcap_event_name"] = enrollment_event
                updates.append(
                    df_participant_update[
                        ["record", "redcap_event_name", "field_name", "value"]
                    ]
                )

    # -------------------------
    # (B) Responder-slot updates (NEW)
    # -------------------------
    if not responder_slots_ready.empty:
        responder_ok = responder_slots_ready.copy()
        responder_ok["secretUserId"] = responder_ok["secretUserId"].astype(str)
        responder_ok["record"] = responder_ok["record"].astype(str)
        responder_ok["redcap_event_name"] = responder_ok["redcap_event_name"].astype(
            str
        )

        # Filter out failures by exact raw secretUserId
        responder_ok = responder_ok[~responder_ok["secretUserId"].isin(raw_failures)]

        if not responder_ok.empty:
            df_responder_update = responder_ok.rename(
                columns={"status_field": "field_name"}
            ).copy()
            df_responder_update["value"] = sent_value

            # Ensure we have event context (should, per your confirmation)
            df_responder_update = df_responder_update[
                df_responder_update["redcap_event_name"].str.strip() != ""
            ]

            if not df_responder_update.empty:
                updates.append(
                    df_responder_update[
                        ["record", "redcap_event_name", "field_name", "value"]
                    ]
                )

    if not updates:
        logger.info("No REDCap records to update.")
        return

    df_update = pd.concat(updates, ignore_index=True)
    df_update = df_update.dropna(
        subset=["record", "redcap_event_name", "field_name", "value"]
    )

    if df_update.empty:
        logger.info("No REDCap records to update after filtering.")
        return

    try:
        rows_updated = redcap_api_push(
            df=df_update[["record", "redcap_event_name", "field_name", "value"]],
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
            {"fields": "ready_to_send_to_curious"},
            filter_logic=f"[record_id] = '{record_id}'",
        )
        if data.empty:
            logger.warning("Could not find record %s to clear flag", record_id)
            return

        event = event_map(data).get("ready_to_send_to_curious")
        if event is None:
            logger.error(
                "Could not determine redcap_event_name for 'ready_to_send_to_curious'. "
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

    Returns:
        List of secretUserIds that failed to send (raw).

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

    Returns:
        List of secretUserIds that failed to send (raw).

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
    data_operations: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Prepare DataFrames with ``accountType`` set for each destination.

    Includes responder invites:
        - reads r_id/r_id_2/r_id_3 + enrollment_complete* from PID 625
        - filters to slots that are "Ready to Send to Curious"
        - looks up responder emails from PID 879 by record_id
        - merges responder accounts into parent_full, then dedupes

    Args:
        curious_data: formatted child and parent DataFrames
        data_operations: raw export data from PID 625 for the record(s)

    Returns:
        (child_full, child_limited, parent_full, responder_slots_ready)

        responder_slots_ready columns:
            - record
            - redcap_event_name
            - secretUserId
            - status_field

    """
    child_full = curious_data["child"].copy()
    child_limited = curious_data["child"].copy()
    parent_full = curious_data["parent"].copy()

    # -----------------------------
    # Responder slots that are ready
    # -----------------------------
    responder_slots_ready = _extract_ready_responder_slots(data_operations)

    if not responder_slots_ready.empty:
        responder_emails = _fetch_responder_emails()

        responder_accounts = responder_slots_ready.merge(
            responder_emails,
            on="secretUserId",
            how="left",
        )[["secretUserId", "email"]]

        responder_accounts = _dedupe_by_best_row(responder_accounts, key="secretUserId")

        # Merge responder accounts into parent_full
        if "secretUserId" in parent_full.columns:
            parent_full["secretUserId"] = parent_full["secretUserId"].astype(str)
            parent_full = parent_full.merge(
                responder_accounts,
                on="secretUserId",
                how="outer",
                suffixes=("", "_responder"),
            )

            # Prefer responder email when present
            if "email_responder" in parent_full.columns:
                if "email" in parent_full.columns:
                    parent_full["email"] = parent_full["email_responder"].combine_first(
                        parent_full["email"]
                    )
                else:
                    parent_full["email"] = parent_full["email_responder"]

                parent_full = parent_full.drop(
                    columns=["email_responder"], errors="ignore"
                )
        else:
            parent_full = responder_accounts.copy()

        # Final dedupe: ensure one account per secretUserId (best row wins)
        parent_full = _dedupe_by_best_row(parent_full, key="secretUserId")

    # ------------------------------------------------------------------
    # Existing parent_involvement filtering logic (unchanged)
    # ------------------------------------------------------------------
    if "parent_involvement" in child_limited.columns:
        is_set = child_limited["parent_involvement"].notna()
        has_one = child_limited["parent_involvement"].apply(_in_set)

        drop_mask = is_set & ~has_one

        if drop_mask.any():
            drop_ids = child_limited.loc[drop_mask, "secretUserId"].apply(
                stringify_secret_user_id
            )

            child_limited = child_limited[~drop_mask]

            if "secretUserId" in parent_full.columns:
                parent_match_ids = (
                    parent_full["secretUserId"]
                    .astype(str)
                    .str.lstrip("rR")
                    .apply(stringify_secret_user_id)
                )
                parent_full = parent_full[~parent_match_ids.isin(drop_ids)]

            child_limited = child_limited.dropna(axis=1, how="all")
            parent_full = parent_full.dropna(axis=1, how="all")

    # ------------------------------------------------------------------
    # Drop internal processing columns
    # ------------------------------------------------------------------
    cols_to_drop = ["parent_involvement", "adult_enrollment_form_complete"]
    child_full = child_full.drop(columns=cols_to_drop, errors="ignore")
    child_limited = child_limited.drop(columns=cols_to_drop, errors="ignore")
    parent_full = parent_full.drop(columns=cols_to_drop, errors="ignore")

    # ------------------------------------------------------------------
    # Set account types
    # ------------------------------------------------------------------
    child_full["accountType"] = "full"
    child_limited["accountType"] = "limited"
    parent_full["accountType"] = "full"

    return child_full, child_limited, parent_full, responder_slots_ready


def _push_to_curious(
    data_operations: pd.DataFrame,
    curious_data: dict[Literal["child", "parent"], pd.DataFrame],
) -> list[str]:
    """
    Validate, push, and update REDCap for a batch of formatted records.

    Returns:
        List of secretUserIds that failed to send (raw).

    """
    child_full, child_limited, parent_full, responder_slots_ready = (
        _prepare_curious_data(
            curious_data,
            data_operations,
        )
    )

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

    update_redcap(
        redcap_df=data_operations,
        curious_child_df=curious_data["child"],
        failures=failures,
        responder_slots_ready=responder_slots_ready,
    )
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
            {"fields": str(Fields.export_operations.for_curious)},
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
            {"fields": str(Fields.export_operations.for_curious)},
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


if __name__ == "__main__":
    main()
