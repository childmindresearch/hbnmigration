"""Monitor Curious account invitations and send updates to REDCap."""

from io import StringIO
import logging
from typing import Literal, Optional

import polars as pl
import requests

from mindlogger_data_export.mindlogger import MindloggerData
from mindlogger_data_export.outputs import NamedOutput, RedcapImportFormat

from .._config_variables import curious_variables, redcap_variables
from ..from_redcap.config import Fields as RedcapFields, Values as RedcapValues
from ..utility_functions import (
    add_cache_keys,
    create_composite_cache_key,
    CuriousDecryptedAnswer,
    CuriousId,
    DataCache,
    fetch_api_data,
    filter_by_cache,
    initialize_logging,
    log_cache_statistics,
    yesterday_or_more_recent,
)
from .config import (
    curious_authenticate,
    Fields as CuriousFields,
    invitation_statuses,
    Values as CuriousValues,
)
from .decryption import decrypt_single, get_applet_encryption
from .utils import deduplicate_dataframe, parse_dt

initialize_logging()
logger = logging.getLogger(__name__)

AccountContext = Literal["responder", "child"]
ACCOUNT_CONTEXTS: list[AccountContext] = ["responder", "child"]
REDCAP_TOKEN: str = ""

# Columns that are internal-only and must never reach REDCap
_INTERNAL_COLUMNS = frozenset(
    {
        "instrument",
        "account_context",
        "respondent_id",
        "has_response",
        "cache_key",
    }
)

# Intermediate columns created during format_for_redcap DataFrame building
_FORMAT_INTERMEDIATE_COLUMNS = frozenset(
    {
        "item_id",
        "item_name",
        "item_prompt",
        "item_type",
        "item_response_options",
        "response_value",
        "response_status",
        "response_raw_score",
        "target_user_secret_id",
        "source_user_secret_id",
        "activity_submission_id",
        "activity_flow_submission_id",
        "activity_start_time",
        "activity_end_time",
        "activity_start_time_str",
        "activity_end_time_str",
        "activity_time_start_time",
        "activity_time_end_time",
    }
)

_APPLET_NAMES: dict[AccountContext, str] = {
    "responder": "Healthy Brain Network Questionnaires",
    "child": "CHILD-Healthy Brain Network Questionnaires",
}


# ---------------------------------------------------------------------------
# Account-context helpers
# ---------------------------------------------------------------------------


def _instrument_for(ctx: AccountContext) -> str:
    return f"curious_account_created_{'responder' if ctx == 'responder' else 'child'}"


def _field_suffix_for(ctx: AccountContext) -> str:
    return "_c" if ctx == "child" else ""


def _prefixed_field(base: str, ctx: AccountContext) -> str:
    return f"curious_account_created_{base}{_field_suffix_for(ctx)}"


def _status_field_for(ctx: AccountContext) -> str:
    return _prefixed_field("invite_status", ctx)


def _response_field_for(ctx: AccountContext) -> str:
    suffix = _field_suffix_for(ctx)
    return f"curious_account_created_account_created_response{suffix}"


def _complete_value(label: str) -> str:
    return RedcapValues.PID625.curious_account_created_responder_complete[label]


def _drop_present(df: pl.DataFrame, candidates: frozenset[str]) -> pl.DataFrame:
    to_drop = [c for c in candidates if c in df.columns]
    return df.drop(to_drop) if to_drop else df


def _null_str() -> pl.Expr:
    return pl.lit(None).cast(pl.String)


def _user_struct(secret_id_expr: pl.Expr | None = None) -> pl.Expr:
    return pl.struct(
        (secret_id_expr if secret_id_expr is not None else _null_str()).alias(
            "secret_id"
        ),
        _null_str().alias("id"),
        _null_str().alias("nickname"),
        _null_str().alias("relation"),
        _null_str().alias("tag"),
    )


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


class Endpoints:
    """Initialized endpoints."""

    Curious = curious_variables.Endpoints()
    Redcap = redcap_variables.Endpoints()


# ---------------------------------------------------------------------------
# Cache helpers
# ---------------------------------------------------------------------------


def create_invitation_cache_key(record_id: str, status: str, has_response: bool) -> str:
    """Create a unique cache key that includes the invitation state."""
    return create_composite_cache_key(record_id, status, has_response)


def _add_cache_keys_to_df(
    df: pl.DataFrame,
    status_field: str | None,
    response_field: str | None,
) -> pl.DataFrame:
    """Add ``cache_key`` column to *df* based on current state."""
    has_response_expr = (
        pl.col(response_field).is_not_null()
        if response_field and response_field in df.columns
        else pl.lit(False)
    )
    df = df.with_columns(has_response_expr.alias("has_response"))

    if status_field and status_field in df.columns:
        df = add_cache_keys(
            df,
            ["record_id", status_field, "has_response"],
            create_invitation_cache_key,
        )
    else:
        df = df.with_columns(
            (
                pl.col("record_id").cast(pl.Utf8)
                + pl.lit("::response:")
                + pl.col("has_response").cast(pl.Utf8)
            ).alias("cache_key")
        )

    return df.drop("has_response")


# ---------------------------------------------------------------------------
# REDCap helpers
# ---------------------------------------------------------------------------


def _redcap_export_params(token: str, **overrides: str) -> dict:
    """Build a standard REDCap export payload, merging in *overrides*."""
    base = {
        "token": token,
        "content": "record",
        "format": "csv",
        "type": "flat",
        "rawOrLabel": "raw",
        "rawOrLabelHeaders": "raw",
        "exportCheckboxLabel": "false",
        "exportSurveyFields": "false",
        "exportDataAccessGroups": "false",
        "returnFormat": "csv",
    }
    base.update(overrides)
    return base


def lookup_mrn_from_r_id(r_id: str, token: str) -> str | None:
    """Look up MRN for a given *r_id* from REDCap PID 625."""
    try:
        data = fetch_api_data(
            Endpoints.Redcap.base_url,
            redcap_variables.headers,
            _redcap_export_params(
                token,
                fields=str(RedcapFields.export_operations.for_mrn_lookup),
            ),
        )
        if data.empty:
            logger.warning("No data returned from REDCap for MRN lookup")
            return None

        matching = data[data["r_id"].astype(str) == str(r_id)]
        if matching.empty:
            logger.debug("No MRN found for r_id: %s", r_id)
            return None

        mrn = matching["record_id"].iloc[0]
        logger.debug("Found MRN %s for r_id %s", mrn, r_id)
        return str(mrn)
    except Exception as e:
        logger.warning("Error looking up MRN for r_id %s: %s", r_id, e)
        return None


def push_to_redcap(
    data: pl.DataFrame | str,
    token: str,
    cache: DataCache | None = None,
) -> int:
    """Push data to REDCap with deduplication."""
    df = pl.read_csv(StringIO(data)) if isinstance(data, str) else data

    instrument = (
        df.select("instrument").to_series()[0] if "instrument" in df.columns else None
    ) or "curious_account_created_responder"

    df = _drop_present(df, _INTERNAL_COLUMNS)

    df, num_duplicates = deduplicate_dataframe(
        df,
        token,
        Endpoints.Redcap.base_url,
        redcap_variables.headers,
        instrument,
    )

    if df.is_empty():
        logger.info("All invitation rows are duplicates, skipping upload")
        return 0

    if num_duplicates > 0:
        logger.info("Removed %d duplicate invitation rows before push", num_duplicates)

    push_data = {
        "token": token,
        "content": "record",
        "action": "import",
        "format": "csv",
        "type": "flat",
        "overwriteBehavior": "normal",
        "forceAutoNumber": "false",
        "data": df.write_csv(),
        "returnContent": "count",
        "returnFormat": "csv",
    }

    r = requests.post(Endpoints.Redcap.base_url, data=push_data)
    if r.status_code != requests.codes["okay"]:
        logger.exception("%s\n%s\nHTTP Status: %d", r.reason, r.text, r.status_code)
    r.raise_for_status()
    return r.json()


def update_already_completed(
    df: pl.DataFrame, account_context: AccountContext, token: str
) -> pl.DataFrame:
    """Filter out records that are already marked complete in REDCap."""
    complete_field = f"{_instrument_for(account_context)}_complete"

    already_completed = fetch_api_data(
        Endpoints.Redcap.base_url,
        redcap_variables.headers,
        _redcap_export_params(
            token,
            action="export",
            type="eav",
            csvDelimiter="",
            fields=complete_field,
            filterLogic=(
                RedcapValues.PID625.curious_account_created_responder_complete.filter_logic(
                    "Complete"
                )
            ),
        ),
        return_type=list,
    )

    return df.filter(~pl.col("record_id").is_in(already_completed)).drop_nulls()


# ---------------------------------------------------------------------------
# Curious helpers
# ---------------------------------------------------------------------------


def create_invitation_record(
    respondent: dict,
    applet_id: CuriousId,
    account_context: AccountContext,
    redcap_token: str,
) -> dict | None:
    """Create a dictionary for a respondent with MRN and invitation status."""
    details: list[dict] = [
        detail for detail in respondent["details"] if detail["appletId"] == applet_id
    ]
    if not details:
        return None

    detail = details[-1]
    secret_id: str = detail["respondentSecretId"]

    try:
        secret_id = str(int(secret_id))
    except ValueError:
        secret_id = str(secret_id)

    instrument = _instrument_for(account_context)
    event_name = "admin_arm_1"

    if account_context == "responder":
        mrn = lookup_mrn_from_r_id(secret_id, redcap_token)
        if not mrn:
            logger.warning(
                "Could not find MRN for responder r_id: %s, skipping", secret_id
            )
            return None
        record_id = mrn
    else:
        record_id = secret_id

    return {
        "record_id": record_id,
        _prefixed_field("source_secret_id", account_context): secret_id,
        _status_field_for(account_context): invitation_statuses[respondent["status"]],
        "redcap_event_name": event_name,
        f"{instrument}_complete": _complete_value("Incomplete"),
        # Internal-only (stripped before upload)
        "respondent_id": detail["subjectId"],
        "instrument": instrument,
        "account_context": account_context,
    }


def check_activity_response(
    token: str,
    respondent: dict,
    applet_id: CuriousId,
    activity_id: CuriousId,
    account_context: AccountContext,
) -> list[NamedOutput]:
    """Check for response to activity."""
    encryption = get_applet_encryption(Endpoints.Curious.applet(applet_id), token)
    response = requests.get(
        Endpoints.Curious.applet_activity_answers_list(applet_id, activity_id)
        + f"?targetSubjectId={respondent['respondent_id']}",
        headers=curious_variables.headers(token),
    )

    if response.status_code != requests.codes["okay"]:
        return []

    result = response.json()["result"]
    if not result:
        return []

    applet_name = respondent.get("applet_name", "Healthy Brain Network Questionnaires")
    password = curious_variables.AppletCredentials()[applet_name]["applet_password"]

    all_formatted: list[NamedOutput] = []
    for answer in result:
        decrypted = decrypt_single(answer, encryption, password)
        formatted = format_for_redcap(decrypted, respondent, account_context)
        if formatted:
            all_formatted.extend(formatted)

    return all_formatted


def check_activity_responses(
    token: str,
    df: pl.DataFrame,
    applet_id: CuriousId,
    activity_id: CuriousId,
    account_context: AccountContext,
) -> pl.DataFrame:
    """Check for responses to activity for all records."""
    responses = []
    for row in df.iter_rows(named=True):
        response = check_activity_response(
            token, row, applet_id, activity_id, account_context
        )
        responses += [r.output for r in response]
    return pl.concat(responses) if responses else df


# ---------------------------------------------------------------------------
# Formatting
# ---------------------------------------------------------------------------


def _strip_instrument_infix(
    col: str,
    account_context: AccountContext,
) -> str:
    """
    Remove the instrument-specific infix that ``RedcapImportFormat`` inserts.

    ``RedcapImportFormat`` produces field names like
    ``curious_account_created_responder_account_created_response``
    but the actual REDCap field is
    ``curious_account_created_account_created_response``.

    The ``_complete`` field keeps its instrument infix because REDCap
    genuinely uses ``curious_account_created_responder_complete``.

    Parameters
    ----------
    col : str
        Column name to transform.
    account_context : AccountContext
        ``"responder"`` or ``"child"``.

    Returns
    -------
    str
        Transformed column name.

    """
    if col.endswith("_complete"):
        return col
    infix = "responder_" if account_context == "responder" else "child_"
    prefix_with = f"curious_account_created_{infix}"
    if col.startswith(prefix_with):
        return f"curious_account_created_{col[len(prefix_with) :]}"
    return col


def _add_child_suffix(col: str, suffix: str) -> str:
    """
    Append *suffix* to ``curious_account_created_`` data fields.

    Leaves ``_complete``, ``record_id``, and ``redcap_event_name`` untouched.

    Parameters
    ----------
    col : str
        Column name to transform.
    suffix : str
        Suffix to append (e.g. ``"_c"``).

    Returns
    -------
    str
        Transformed column name.

    """
    if (
        col.startswith("curious_account_created_")
        and not col.endswith("_complete")
        and col not in ("record_id", "redcap_event_name")
    ):
        return f"{col}{suffix}"
    return col


def format_for_redcap(
    ml_data: CuriousDecryptedAnswer,
    redcap_context: dict,
    account_context: AccountContext,
) -> list[NamedOutput]:
    """Format response data for REDCap import."""
    if not ml_data:
        return []

    record_id = redcap_context["record_id"]
    redcap_event_name: str = redcap_context["redcap_event_name"]
    instrument = redcap_context["instrument"]
    field_suffix = _field_suffix_for(account_context)

    submit_id = ml_data.get("submitId", instrument)
    start_time = ml_data["startDatetime"]
    end_time = ml_data["endDatetime"]
    answers = ml_data.get("answer", [])

    rows: list[dict] = [
        {
            "item_id": item["id"],
            "item_name": item["name"],
            "item_prompt": item["question"].get("en", ""),
            "item_type": item["responseType"],
            "item_response_options": _build_response_options(item),
            "response_value": _build_response_value(
                item, answers[idx] if idx < len(answers) else {}
            ),
            "response_status": "completed",
            "response_raw_score": None,
            "activity_start_time": start_time,
            "activity_end_time": end_time,
            "utc_timezone_offset": 0,
            "applet_version": ml_data.get("version", ""),
            "target_user_secret_id": ml_data.get("respondentSecretId", ""),
            "source_user_secret_id": ml_data.get("sourceSecretId", ""),
            "activity_submission_id": submit_id,
            "activity_flow_submission_id": submit_id,
        }
        for idx, item in enumerate(ml_data.get("items", []))
    ]

    instrument_label = instrument.replace("_", " ").title()

    df = pl.DataFrame(rows).with_columns(
        pl.col("activity_start_time").alias("activity_start_time_str"),
        pl.col("activity_end_time").alias("activity_end_time_str"),
        parse_dt("activity_start_time").alias("activity_start_time"),
        parse_dt("activity_end_time").alias("activity_end_time"),
        pl.duration(milliseconds=pl.col("utc_timezone_offset")).alias(
            "utc_timezone_offset"
        ),
        pl.struct(
            pl.lit(instrument).alias("id"),
            pl.lit(instrument_label).alias("name"),
        ).alias("activity"),
        pl.struct(
            pl.lit(instrument).alias("id"),
            pl.lit(instrument_label).alias("name"),
            pl.col("activity_flow_submission_id").alias("submission_id"),
        ).alias("activity_flow"),
        pl.struct(
            pl.col("activity_submission_id").alias("id"),
            _null_str().alias("review_id"),
        ).alias("activity_submission"),
        pl.struct(
            _null_str().alias("id"),
            _null_str().alias("history_id"),
            pl.lit(None).cast(pl.Datetime("ms", "UTC")).alias("start_time"),
        ).alias("activity_schedule"),
        pl.struct(
            pl.col("item_id").alias("id"),
            pl.col("item_name").alias("name"),
            pl.col("item_prompt").alias("prompt"),
            pl.col("item_type").alias("type"),
            _null_str().alias("raw_options"),
            pl.col("item_response_options").alias("response_options"),
        ).alias("item"),
        pl.struct(
            pl.col("response_status").alias("status"),
            pl.col("response_raw_score").alias("raw_score"),
            _null_str().alias("raw_response"),
            pl.col("response_value").alias("value"),
        ).alias("response"),
        _user_struct(pl.col("target_user_secret_id")).alias("target_user"),
        _user_struct(pl.col("source_user_secret_id")).alias("source_user"),
        _user_struct().alias("input_user"),
        _user_struct().alias("account_user"),
    )

    df = df.with_columns(
        pl.struct(
            parse_dt("activity_start_time_str").alias("start_time"),
            parse_dt("activity_end_time_str").alias("end_time"),
        ).alias("activity_time"),
    )

    df = _drop_present(df, _FORMAT_INTERMEDIATE_COLUMNS)

    formatter = RedcapImportFormat(project={instrument: redcap_event_name})
    results = formatter.produce(MindloggerData(df))

    valid_fields = CuriousFields.for_context(account_context)

    for result in results:
        # Strip the instrument infix (responder_ / child_) that the formatter
        # inserts, except for _complete which genuinely carries it in REDCap.
        result.output = result.output.rename(
            lambda col: _strip_instrument_infix(col, account_context)
        )

        # Append _c suffix for child data fields
        if field_suffix:
            result.output = result.output.rename(
                lambda col: _add_child_suffix(col, field_suffix)
            )

        context_columns: list[pl.Expr] = [
            pl.lit(record_id).alias("record_id"),
            pl.lit(redcap_event_name).alias("redcap_event_name"),
        ]

        for short_key, full_key in {
            "source_secret_id": _prefixed_field("source_secret_id", account_context),
            "invite_status": _prefixed_field("invite_status", account_context),
        }.items():
            if short_key in redcap_context:
                context_columns.append(
                    pl.lit(redcap_context[short_key]).alias(full_key)
                )

        response_col = _response_field_for(account_context)
        complete_field = f"{instrument}_complete"

        hbnq = CuriousValues.HealthyBrainNetworkQuestionnaires
        if response_col in result.output.columns:
            confirmed = hbnq.CuriousAccountCreated.acount_created[
                "I confirm that I have created a Curious account"
            ]
            context_columns.append(
                pl.when(pl.col(response_col).cast(pl.Utf8) == confirmed)
                .then(pl.lit(_complete_value("Unverified")))
                .otherwise(pl.lit(_complete_value("Incomplete")))
                .alias(complete_field)
            )
        else:
            label = (
                "Unverified"
                if redcap_context.get("invite_status") == "3"
                else "Incomplete"
            )
            context_columns.append(pl.lit(_complete_value(label)).alias(complete_field))

        result.output = result.output.with_columns(context_columns)

        # Only keep fields that actually exist in the REDCap project
        keep = [c for c in valid_fields if c in result.output.columns]
        result.output = result.output.select(keep)

    return results


def _build_response_value(item: dict, answer_value: dict) -> dict:
    """Build the ``response_value`` dict for a single item."""
    response_val = answer_value.get("value")
    return {
        "type": item["responseType"],
        "raw_value": str(response_val) if response_val is not None else None,
        "null_value": response_val is None,
        "single_value": response_val if isinstance(response_val, int) else None,
        "value": response_val if isinstance(response_val, list) else None,
        "text": response_val if isinstance(response_val, str) else None,
        "file": None,
        "date": None,
        "time": None,
        "time_range": None,
        "geo": None,
        "matrix": None,
        "optional_text": None,
        "subscale": None,
    }


def _build_response_options(item: dict) -> list[dict]:
    """Extract normalised response options from an item definition."""
    if "responseValues" not in item or "options" not in item["responseValues"]:
        return []
    return [
        {
            "name": opt.get("text", ""),
            "value": opt.get("value", 0),
            "score": opt.get("score", 0) if opt.get("score") is not None else 0,
        }
        for opt in item["responseValues"]["options"]
    ]


# ---------------------------------------------------------------------------
# Pulling from Curious
# ---------------------------------------------------------------------------


def pull_data_from_curious(
    token: str,
    applet_name: str,
    account_context: AccountContext,
    redcap_token: str,
) -> pl.DataFrame:
    """Pull data from Curious and construct a Polars DataFrame."""
    applet_id = curious_variables.applets[applet_name].applet_id
    owner_id = curious_variables.owner_ids.get(
        "Healthy Brain Network (HBN)",
        next(iter(curious_variables.owner_ids.values())),
    )

    response = requests.get(
        Endpoints.Curious.invitation_statuses(owner_id, applet_id),
        headers=curious_variables.headers(token),
    )
    response.raise_for_status()

    records = []
    for respondent in response.json().get("result", []):
        last_seen = respondent.get("lastSeen")
        if last_seen is None or yesterday_or_more_recent(last_seen):
            respondent["applet_name"] = applet_name
            record = create_invitation_record(
                respondent, applet_id, account_context, redcap_token
            )
            if record is not None:
                records.append(record)

    invitation_df = pl.DataFrame(records)
    if not invitation_df.is_empty():
        invitation_df = update_already_completed(
            invitation_df, account_context, redcap_token
        )
    return invitation_df


# ---------------------------------------------------------------------------
# Token helper
# ---------------------------------------------------------------------------


def _get_target_token(target_pid: Literal[625, 891]) -> str | None:
    """Get REDCap token for *target_pid*."""
    if target_pid == 891:  # noqa: PLR2004
        try:
            token = redcap_variables.Tokens().pid891
            if token is None:
                msg = "PID 891 token is None"
                raise AttributeError(msg)
            return token
        except AttributeError:
            logger.warning(
                "PID 891 not yet configured, skipping. "
                "This is expected during transition period."
            )
            return None
    return redcap_variables.Tokens().pid625


# ---------------------------------------------------------------------------
# Generic account processor
# ---------------------------------------------------------------------------


def _process_accounts(  # noqa: PLR0913
    applet_name: str,
    account_context: AccountContext,
    token: str,
    lookup_token: str,
    cache: DataCache,
    target_pid: Literal[625, 891],
) -> None:
    """Process invitations for a single account context."""
    try:
        auth = curious_authenticate(applet_name)
    except (KeyError, ConnectionError) as e:
        logger.warning(
            "%s applet not configured: %s. Skipping.",
            account_context.capitalize(),
            e,
        )
        return

    invitation_df = pull_data_from_curious(
        auth.access, applet_name, account_context, lookup_token
    )
    if invitation_df.is_empty():
        logger.info("No %s invitations to update.", account_context)
        return

    status_field = _status_field_for(account_context)
    response_field = _response_field_for(account_context)

    # Pre-response cache check
    invitation_df = _add_cache_keys_to_df(invitation_df, status_field, None)
    invitation_df = filter_by_cache(
        invitation_df, cache, "cache_key", logger, f"{account_context} records"
    )
    if invitation_df.is_empty():
        return

    # Check Curious for activity responses
    applet_cfg = curious_variables.applets[applet_name]
    invitation_df = check_activity_responses(
        auth.access,
        invitation_df,
        applet_cfg.applet_id,
        applet_cfg.activities["Curious Account Created"].activity_id,
        account_context,
    )

    # Post-response cache keys (status column may have been lost after formatting)
    actual_status = next(
        (
            c
            for c in (
                status_field,
                *(c for c in invitation_df.columns if "invite_status" in c),
            )
            if c in invitation_df.columns
        ),
        None,
    )
    invitation_df = _add_cache_keys_to_df(invitation_df, actual_status, response_field)

    invitation_df = invitation_df.unique(subset=["record_id"], keep="last")
    n_records = push_to_redcap(invitation_df.drop("cache_key"), token, cache)
    logger.info(
        "%d %s records updated in REDCap (PID %d)",
        n_records,
        account_context,
        target_pid,
    )

    if n_records > 0:
        cache.bulk_mark_processed(
            invitation_df["cache_key"].to_list(),
            metadata={"count": n_records, "type": account_context},
        )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main(
    applet_name: Optional[str] = None,
    target_pid: Literal[625, 891] = 625,
    account_context: Optional[AccountContext] = None,
) -> None:
    """Monitor Curious account invitations and send updates to REDCap."""
    token = _get_target_token(target_pid)
    if token is None:
        return

    lookup_token = redcap_variables.Tokens().pid625
    cache = DataCache(f"curious_invitations_to_redcap_{target_pid}", ttl_minutes=2)

    for ctx in ACCOUNT_CONTEXTS:
        _process_accounts(
            applet_name or _APPLET_NAMES[ctx],
            ctx,
            token,
            lookup_token,
            cache,
            target_pid,
        )

    log_cache_statistics(cache, logger)


if __name__ == "__main__":
    main()
