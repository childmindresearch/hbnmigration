"""Monitor Curious account invitations and send updates to REDCap."""

from dataclasses import dataclass
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
    redact_secret,
    yesterday_or_more_recent,
)
from .config import (
    curious_authenticate,
    Fields as CuriousFields,
    invitation_statuses,
    Values as CuriousValues,
)
from .decryption import decrypt_single, get_applet_encryption
from .utils import deduplicate_dataframe, parse_dt, REDCAP_ENDPOINTS

initialize_logging()
logger = logging.getLogger(__name__)

AccountContext = Literal["responder", "child"]
ACCOUNT_CONTEXTS: list[AccountContext] = ["responder", "child"]

TargetPid = Literal[625, 891]

_INTERNAL_COLUMNS = frozenset(
    {
        "instrument",
        "account_context",
        "respondent_id",
        "has_response",
        "cache_key",
    }
)

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
    """REDCap instrument name for *ctx*."""
    return f"curious_account_created_{'responder' if ctx == 'responder' else 'child'}"


def _field_suffix_for(ctx: AccountContext) -> str:
    """``'_c'`` for child, empty string otherwise."""
    return "_c" if ctx == "child" else ""


def _prefixed_field(base: str, ctx: AccountContext) -> str:
    """Full ``curious_account_created_`` field name."""
    return f"curious_account_created_{base}{_field_suffix_for(ctx)}"


def _status_field_for(ctx: AccountContext) -> str:
    """Invite-status field for *ctx*."""
    return _prefixed_field("invite_status", ctx)


def _response_field_for(ctx: AccountContext) -> str:
    """Account-created response field for *ctx*."""
    return f"curious_account_created_account_created_response{_field_suffix_for(ctx)}"


def _complete_value(label: str) -> str:
    """Map human label to REDCap coded value."""
    return RedcapValues.PID625.curious_account_created_responder_complete[label]


def _drop_present(df: pl.DataFrame, candidates: frozenset[str]) -> pl.DataFrame:
    """Drop columns from *df* that appear in *candidates*."""
    to_drop = [c for c in candidates if c in df.columns]
    return df.drop(to_drop) if to_drop else df


def _null_str() -> pl.Expr:
    """``Null`` literal cast to ``String``."""
    return pl.lit(None).cast(pl.String)


def _user_struct(secret_id_expr: pl.Expr | None = None) -> pl.Expr:
    """User-struct expression for the formatter."""
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
    Redcap = REDCAP_ENDPOINTS


# ---------------------------------------------------------------------------
# Cache helpers
# ---------------------------------------------------------------------------


def create_invitation_cache_key(record_id: str, status: str, has_response: bool) -> str:
    """Return unique cache key including invitation state."""
    return create_composite_cache_key(record_id, status, has_response)


def _add_cache_keys_to_df(
    df: pl.DataFrame,
    status_field: str | None,
    response_field: str | None,
) -> pl.DataFrame:
    """Add ``cache_key`` column based on current state."""
    has_resp = (
        pl.col(response_field).is_not_null()
        if response_field and response_field in df.columns
        else pl.lit(False)
    )
    df = df.with_columns(has_resp.alias("has_response"))
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
            ).alias("cache_key"),
        )
    return df.drop("has_response")


# ---------------------------------------------------------------------------
# REDCap helpers
# ---------------------------------------------------------------------------


def _redcap_export_params(token: str, **overrides: str) -> dict:
    """Export standard REDCap payload with *overrides*."""
    return {
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
        **overrides,
    }


def lookup_mrn_from_r_id(r_id: str, token: str) -> str | None:
    """Look up MRN for *r_id* from REDCap PID 625."""
    try:
        data = fetch_api_data(
            Endpoints.Redcap.base_url,
            redcap_variables.headers,
            _redcap_export_params(
                token, fields=str(RedcapFields.export_operations.for_mrn_lookup)
            ),
        )
        if data.empty:
            return None
        matching = data[data["r_id"].astype(str) == str(r_id)]
        return str(matching["record_id"].iloc[0]) if not matching.empty else None
    except Exception as e:
        logger.warning("MRN lookup error: %s", e)
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
    df, n_dup = deduplicate_dataframe(
        df,
        token,
        Endpoints.Redcap.base_url,
        redcap_variables.headers,
        instrument,
    )
    if df.is_empty():
        logger.info("All invitation rows are duplicates, skipping")
        return 0
    if n_dup:
        logger.info("Removed %d duplicate invitation rows", n_dup)
    r = requests.post(
        Endpoints.Redcap.base_url,
        data={
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
        },
    )
    if r.status_code != requests.codes["okay"]:
        logger.exception("%s\n%s\nHTTP %d", r.reason, r.text, r.status_code)
    r.raise_for_status()
    return r.json()


def update_already_completed(
    df: pl.DataFrame,
    account_context: AccountContext,
    token: str,
) -> pl.DataFrame:
    """Filter out records already marked complete in REDCap."""
    complete_field = f"{_instrument_for(account_context)}_complete"
    already = fetch_api_data(
        Endpoints.Redcap.base_url,
        redcap_variables.headers,
        _redcap_export_params(
            token,
            action="export",
            type="eav",
            csvDelimiter="",
            fields=complete_field,
            filterLogic=RedcapValues.PID625.curious_account_created_responder_complete.filter_logic(
                "Complete"
            ),
        ),
        return_type=list,
    )
    return df.filter(~pl.col("record_id").is_in(already)).drop_nulls()


# ---------------------------------------------------------------------------
# Curious helpers
# ---------------------------------------------------------------------------


def create_invitation_record(
    respondent: dict,
    applet_id: CuriousId,
    account_context: AccountContext,
    redcap_token: str,
) -> dict | None:
    """Create a dict for a respondent with MRN and invitation status."""
    details = [d for d in respondent["details"] if d["appletId"] == applet_id]
    if not details:
        return None
    detail = details[-1]
    secret_id = detail["respondentSecretId"]
    try:
        secret_id = str(int(secret_id))
    except ValueError:
        secret_id = str(secret_id)

    instrument = _instrument_for(account_context)
    if account_context == "responder":
        mrn = lookup_mrn_from_r_id(secret_id, redcap_token)
        if not mrn:
            logger.warning(
                "No MRN for responder r_id %s, skipping", redact_secret(secret_id)
            )
            return None
        record_id = mrn
    else:
        record_id = secret_id

    return {
        "record_id": record_id,
        _prefixed_field("source_secret_id", account_context): secret_id,
        _status_field_for(account_context): invitation_statuses[respondent["status"]],
        "redcap_event_name": "admin_arm_1",
        f"{instrument}_complete": _complete_value("Incomplete"),
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
    resp = requests.get(
        Endpoints.Curious.applet_activity_answers_list(applet_id, activity_id)
        + f"?targetSubjectId={respondent['respondent_id']}",
        headers=curious_variables.headers(token),
    )
    if resp.status_code != requests.codes["okay"]:
        return []
    result = resp.json()["result"]
    if not result:
        return []
    applet_name = respondent.get("applet_name", "Healthy Brain Network Questionnaires")
    password = curious_variables.AppletCredentials()[applet_name]["applet_password"]
    formatted: list[NamedOutput] = []
    for answer in result:
        out = format_for_redcap(
            decrypt_single(answer, encryption, password), respondent, account_context
        )
        if out:
            formatted.extend(out)
    return formatted


def check_activity_responses(
    token: str,
    df: pl.DataFrame,
    applet_id: CuriousId,
    activity_id: CuriousId,
    account_context: AccountContext,
) -> pl.DataFrame:
    """Check for responses for all records."""
    dfs = [
        r.output
        for row in df.iter_rows(named=True)
        for r in check_activity_response(
            token, row, applet_id, activity_id, account_context
        )
    ]
    return pl.concat(dfs) if dfs else df


# ---------------------------------------------------------------------------
# Formatting
# ---------------------------------------------------------------------------


def _strip_instrument_infix(col: str, account_context: AccountContext) -> str:
    """Remove the formatter's instrument infix (except ``_complete``)."""
    if col.endswith("_complete"):
        return col
    infix = "responder_" if account_context == "responder" else "child_"
    prefix = f"curious_account_created_{infix}"
    return (
        f"curious_account_created_{col[len(prefix) :]}"
        if col.startswith(prefix)
        else col
    )


def _add_child_suffix(col: str, suffix: str) -> str:
    """Append *suffix* to data fields; leave ``_complete`` / standard fields alone."""
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
    event: str = redcap_context["redcap_event_name"]
    instrument = redcap_context["instrument"]
    suffix = _field_suffix_for(account_context)
    submit_id = ml_data.get("submitId", instrument)
    start, end = ml_data["startDatetime"], ml_data["endDatetime"]
    answers = ml_data.get("answer", [])

    rows: list[dict] = [
        {
            "item_id": item["id"],
            "item_name": item["name"],
            "item_prompt": item["question"].get("en", ""),
            "item_type": item["responseType"],
            "item_response_options": _build_response_options(item),
            "response_value": _build_response_value(
                item, answers[i] if i < len(answers) else {}
            ),
            "response_status": "completed",
            "response_raw_score": None,
            "activity_start_time": start,
            "activity_end_time": end,
            "utc_timezone_offset": 0,
            "applet_version": ml_data.get("version", ""),
            "target_user_secret_id": ml_data.get("respondentSecretId", ""),
            "source_user_secret_id": ml_data.get("sourceSecretId", ""),
            "activity_submission_id": submit_id,
            "activity_flow_submission_id": submit_id,
        }
        for i, item in enumerate(ml_data.get("items", []))
    ]
    label = instrument.replace("_", " ").title()
    df = pl.DataFrame(rows).with_columns(
        pl.col("activity_start_time").alias("activity_start_time_str"),
        pl.col("activity_end_time").alias("activity_end_time_str"),
        parse_dt("activity_start_time").alias("activity_start_time"),
        parse_dt("activity_end_time").alias("activity_end_time"),
        pl.duration(milliseconds=pl.col("utc_timezone_offset")).alias(
            "utc_timezone_offset"
        ),
        pl.struct(pl.lit(instrument).alias("id"), pl.lit(label).alias("name")).alias(
            "activity"
        ),
        pl.struct(
            pl.lit(instrument).alias("id"),
            pl.lit(label).alias("name"),
            pl.col("activity_flow_submission_id").alias("submission_id"),
        ).alias("activity_flow"),
        pl.struct(
            pl.col("activity_submission_id").alias("id"), _null_str().alias("review_id")
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
    formatter = RedcapImportFormat(project={instrument: event})
    results = formatter.produce(MindloggerData(df))
    valid_fields = CuriousFields.for_context(account_context)

    for result in results:
        result.output = result.output.rename(
            lambda c: _strip_instrument_infix(c, account_context)
        )
        if suffix:
            result.output = result.output.rename(lambda c: _add_child_suffix(c, suffix))
        ctx_cols: list[pl.Expr] = [
            pl.lit(record_id).alias("record_id"),
            pl.lit(event).alias("redcap_event_name"),
        ]
        for short, full in {
            "source_secret_id": _prefixed_field("source_secret_id", account_context),
            "invite_status": _prefixed_field("invite_status", account_context),
        }.items():
            if short in redcap_context:
                ctx_cols.append(pl.lit(redcap_context[short]).alias(full))
        resp_col = _response_field_for(account_context)
        complete_field = f"{instrument}_complete"
        hbnq = CuriousValues.HealthyBrainNetworkQuestionnaires
        if resp_col in result.output.columns:
            confirmed = hbnq.CuriousAccountCreated.acount_created[
                "I confirm that I have created a Curious account"
            ]
            ctx_cols.append(
                pl.when(pl.col(resp_col).cast(pl.Utf8) == confirmed)
                .then(pl.lit(_complete_value("Unverified")))
                .otherwise(pl.lit(_complete_value("Incomplete")))
                .alias(complete_field),
            )
        else:
            lbl = (
                "Unverified"
                if redcap_context.get("invite_status") == "3"
                else "Incomplete"
            )
            ctx_cols.append(pl.lit(_complete_value(lbl)).alias(complete_field))
        result.output = result.output.with_columns(ctx_cols)
        result.output = result.output.select(
            [c for c in valid_fields if c in result.output.columns]
        )
    return results


def _build_response_value(item: dict, answer_value: dict) -> dict:
    """``response_value`` dict for a single item."""
    v = answer_value.get("value")
    return {
        "type": item["responseType"],
        "raw_value": str(v) if v is not None else None,
        "null_value": v is None,
        "single_value": v if isinstance(v, int) else None,
        "value": v if isinstance(v, list) else None,
        "text": v if isinstance(v, str) else None,
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
    """Normalised response options from an item definition."""
    opts = (item.get("responseValues") or {}).get("options", [])
    return [
        {
            "name": o.get("text", ""),
            "value": o.get("value", 0),
            "score": o.get("score", 0) if o.get("score") is not None else 0,
        }
        for o in opts
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
    """Pull invitation data from Curious."""
    applet_id = curious_variables.applets[applet_name].applet_id
    owner_id = curious_variables.owner_ids.get(
        "Healthy Brain Network (HBN)",
        next(iter(curious_variables.owner_ids.values())),
    )
    resp = requests.get(
        Endpoints.Curious.invitation_statuses(owner_id, applet_id),
        headers=curious_variables.headers(token),
    )
    resp.raise_for_status()
    records = []
    for respondent in resp.json().get("result", []):
        last_seen = respondent.get("lastSeen")
        if last_seen is None or yesterday_or_more_recent(last_seen):
            respondent["applet_name"] = applet_name
            rec = create_invitation_record(
                respondent, applet_id, account_context, redcap_token
            )
            if rec is not None:
                records.append(rec)
    df = pl.DataFrame(records)
    if not df.is_empty():
        df = update_already_completed(df, account_context, redcap_token)
    return df


# ---------------------------------------------------------------------------
# Token helper
# ---------------------------------------------------------------------------


def _get_target_token(target_pid: TargetPid) -> str | None:
    """Get REDCap token for *target_pid*."""
    if target_pid == 891:  # noqa: PLR2004
        try:
            tok = redcap_variables.Tokens().pid891
            if tok is None:
                msg = "PID 891 token is None"
                raise AttributeError(msg)
            return tok
        except AttributeError:
            logger.warning("PID 891 not configured, skipping.")
            return None
    return redcap_variables.Tokens().pid625


# ---------------------------------------------------------------------------
# Generic account processor
# ---------------------------------------------------------------------------


@dataclass
class _ProcessCtx:
    """Bundles arguments for :func:`_process_accounts`."""

    applet_name: str
    account_context: AccountContext
    token: str
    lookup_token: str
    cache: DataCache
    target_pid: TargetPid


def _process_accounts(ctx: _ProcessCtx) -> None:
    """Process invitations for one account context / target PID."""
    try:
        auth = curious_authenticate(ctx.applet_name)
    except (KeyError, ConnectionError) as e:
        logger.warning(
            "%s applet not configured: %s. Skipping.",
            ctx.account_context.capitalize(),
            e,
        )
        return
    inv_df = pull_data_from_curious(
        auth.access, ctx.applet_name, ctx.account_context, ctx.lookup_token
    )
    if inv_df.is_empty():
        logger.info("No %s invitations to update.", ctx.account_context)
        return
    status_field = _status_field_for(ctx.account_context)
    response_field = _response_field_for(ctx.account_context)
    inv_df = _add_cache_keys_to_df(inv_df, status_field, None)
    inv_df = filter_by_cache(
        inv_df, ctx.cache, "cache_key", logger, f"{ctx.account_context} records"
    )
    if inv_df.is_empty():
        return
    applet_cfg = curious_variables.applets[ctx.applet_name]
    inv_df = check_activity_responses(
        auth.access,
        inv_df,
        applet_cfg.applet_id,
        applet_cfg.activities["Curious Account Created"].activity_id,
        ctx.account_context,
    )
    actual_status = next(
        (
            c
            for c in (
                status_field,
                *(c for c in inv_df.columns if "invite_status" in c),
            )
            if c in inv_df.columns
        ),
        None,
    )
    inv_df = _add_cache_keys_to_df(inv_df, actual_status, response_field)
    inv_df = inv_df.unique(subset=["record_id"], keep="last")
    n = push_to_redcap(inv_df.drop("cache_key"), ctx.token, ctx.cache)
    logger.info(
        "%d %s records updated in PID %d", n, ctx.account_context, ctx.target_pid
    )
    if n > 0:
        ctx.cache.bulk_mark_processed(
            inv_df["cache_key"].to_list(),
            metadata={"count": n, "type": ctx.account_context},
        )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main(
    applet_name: Optional[str] = None,
    account_context: Optional[AccountContext] = None,
) -> None:
    """Send ``curious_account_created`` to both PID 625 and PID 891."""
    lookup_token = redcap_variables.Tokens().pid625
    contexts = [account_context] if account_context else ACCOUNT_CONTEXTS
    for pid in (625, 891):
        token = _get_target_token(pid)
        if token is None:
            continue
        cache = DataCache(f"curious_invitations_to_redcap_{pid}", ttl_minutes=2)
        for ctx in contexts:
            _process_accounts(
                _ProcessCtx(
                    applet_name=applet_name or _APPLET_NAMES[ctx],
                    account_context=ctx,
                    token=token,
                    lookup_token=lookup_token,
                    cache=cache,
                    target_pid=pid,
                )
            )
        log_cache_statistics(cache, logger)


if __name__ == "__main__":
    main()
