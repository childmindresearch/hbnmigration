"""Send Curious data to REDCap."""

import csv
from datetime import datetime
import logging
import os
from pathlib import Path
import re
import sys
from tempfile import NamedTemporaryFile, TemporaryDirectory
import time
from typing import Literal

import pandas as pd
import polars as pl
import requests

from mindlogger_data_export.mindlogger import MindloggerData
from mindlogger_data_export.outputs import NamedOutput, RedcapImportFormat

from .._config_variables import curious_variables, redcap_variables
from ..config import Config
from ..exceptions import NoData
from ..from_redcap.config import FieldList
from ..from_redcap.from_redcap import fetch_data
from ..utility_functions import (
    CliOptions,
    compute_dataframe_hash,
    create_composite_cache_key,
    DataCache,
    Endpoints,
    fetch_api_data,
    get_recent_time_window,
    get_redcap_event_names,
    initialize_logging,
    InstrumentRowCount,
    log_cache_statistics,
    Results,
    tsx,
    YESTERDAY,
)
from .utils import (
    deduplicate_dataframe,
    get_alert_field_event,
    get_redcap_records_for_instrument,
    get_redcap_token,
    map_mrns_to_records,
    possible_alert_instruments,
    REDCAP_ENDPOINTS,
    STANDARD_FIELDS,
)

initialize_logging()
logger = logging.getLogger(__name__)

APPLET_CREDENTIALS = curious_variables.AppletCredentials()
"""Initialized credentials."""

ENDPOINTS: dict[Literal["Curious", "REDCap"], Endpoints] = {
    "Curious": curious_variables.Endpoints(),
    "REDCap": REDCAP_ENDPOINTS,
}
"""Initialized endpoints."""

TargetPid = Literal[625, 891]
"""REDCap project IDs that receive Curious data."""

_ACCOUNT_CREATED_PREFIXES = (
    "curious_account_created_responder",
    "curious_account_created_child",
)
"""Instrument-name prefixes that belong exclusively to PID 625."""


def _is_account_created_instrument(name: str) -> bool:
    """Return ``True`` when *name* is a ``curious_account_created`` instrument."""
    return name.replace("_redcap", "").lower().startswith(_ACCOUNT_CREATED_PREFIXES)


# ============================================================================
# Received-flag helpers (PID 625 only)
# ============================================================================


def _instrument_received_field(instrument_name: str) -> str:
    """
    Derive ``<short>_received`` for PID 625.

    Strips trailing age (``_1117``, ``_18+``) and informant (``_sr``,
    ``_pr``) suffixes, then appends ``_received``.
    """
    name = instrument_name.lower()
    name = re.sub(r"_\d+\+?$", "", name)
    name = re.sub(r"_(?:sr|pr)$", "", name)
    return f"{name}_received"


def _build_received_flags_df(outputs: list[NamedOutput]) -> pl.DataFrame | None:
    """
    Build ``_received = "1"`` rows for PID 625 from non-account-created outputs.

    Returns ``None`` when nothing needs flagging.
    """
    rows: list[dict[str, str]] = []
    for output in outputs:
        inst = output.name.replace("_redcap", "")
        if _is_account_created_instrument(inst):
            continue
        df = output.output
        if "record_id" not in df.columns or df.is_empty():
            continue
        field = _instrument_received_field(inst)
        rows.extend(
            {"record_id": str(rid), field: "1", "redcap_event_name": "admin_arm_1"}
            for rid in df["record_id"].unique().to_list()
        )
    return pl.DataFrame(rows) if rows else None


# ============================================================================
# Output splitting by target PID
# ============================================================================


def _split_outputs_by_pid(
    outputs: list[NamedOutput],
    instrument_row_count: dict[str, int],
) -> dict[TargetPid, tuple[list[NamedOutput], dict[str, int]]]:
    """``curious_account_created`` → 625, everything else → 891."""
    out: dict[TargetPid, tuple[list[NamedOutput], dict[str, int]]] = {
        625: ([], {}),
        891: ([], {}),
    }
    for output in outputs:
        inst = output.name.replace("_redcap", "")
        pid: TargetPid = 625 if _is_account_created_instrument(inst) else 891
        out[pid][0].append(output)
        out[pid][1][inst] = instrument_row_count.get(inst, 0)
    return out


# ============================================================================
# Cache Key Utilities
# ============================================================================


def create_instrument_cache_key(
    instrument_name: str,
    file_hash: str,
    row_count: int,
) -> str:
    """Create a unique cache key for instrument data."""
    return create_composite_cache_key(instrument_name, file_hash, row_count)


# ============================================================================
# Data Formatting
# ============================================================================


def _extract_field_names_from_outputs(outputs: list[NamedOutput]) -> set[str]:
    """Extract field names that may need choice metadata."""
    return {
        col
        for output in outputs
        for col in output.output.columns
        if col.endswith(("_response", "_index", "_score"))
    }


def _fetch_redcap_metadata_for_fields(
    pid: int,
    field_names: set[str],
) -> pl.DataFrame | None:
    """Fetch REDCap metadata for *field_names* (falls back to full fetch)."""
    if not field_names:
        return None
    logger.info("Fetching REDCap metadata for %d specific fields…", len(field_names))
    try:
        md = fetch_api_data(
            REDCAP_ENDPOINTS.base_url,
            redcap_variables.headers,
            {
                "token": get_redcap_token(pid),
                "content": "metadata",
                "format": "csv",
                "fields": ",".join(sorted(field_names)),
            },
        )
        if md.empty:
            return None
        df = pl.from_pandas(md)
        logger.info("Loaded metadata for %d/%d fields", len(df), len(field_names))
        return df
    except Exception as e:
        logger.warning("Field-specific metadata failed (%s), falling back…", e)
        return _fetch_redcap_metadata_all(pid)


def _fetch_redcap_metadata_all(pid: int) -> pl.DataFrame | None:
    """Fetch all REDCap metadata (fallback)."""
    md = fetch_api_data(
        REDCAP_ENDPOINTS.base_url,
        redcap_variables.headers,
        {"token": get_redcap_token(pid), "content": "metadata", "format": "csv"},
    )
    return pl.from_pandas(md) if not md.empty else None


# ============================================================================
# Parent-record filtering
# ============================================================================


def _filter_p_suffix(df: pl.DataFrame, col: str, instrument_name: str) -> pl.DataFrame:
    """Remove rows where *col* ends with ``_P``; log what was dropped."""
    mask = pl.col(col).cast(pl.Utf8).str.ends_with("_P")
    dropped = df.filter(mask)
    if len(dropped):
        logger.info(
            "Filtered %d '_P' records in %s: %s",
            len(dropped),
            instrument_name,
            ", ".join(dropped[col].cast(pl.Utf8).to_list()),
        )
    return df.filter(~mask)


def _filter_parent_records(output: NamedOutput) -> NamedOutput:
    """Filter ``_P``-suffix records from *output*."""
    df, name = output.output, output.name
    if name.startswith("curious_account_created_responder"):
        return output
    for col in ("target_user_secret_id", "record_id"):
        if col in df.columns:
            return NamedOutput(name=name, output=_filter_p_suffix(df, col, name))
    logger.exception("Cannot filter '%s': no ID column found", name)
    return output


# ============================================================================
# Formatting entry point
# ============================================================================


def format_for_redcap(
    pid: int,
    curious_data_dir: Path,
) -> tuple[list[NamedOutput], InstrumentRowCount]:
    """Format Curious data for REDCap import."""
    event_names = get_redcap_event_names(
        REDCAP_ENDPOINTS.base_url,
        redcap_variables.headers,
        {"token": get_redcap_token(pid)},
    )
    formatter_init = RedcapImportFormat(project=event_names, redcap_metadata=None)
    try:
        ml_data = MindloggerData.create(curious_data_dir)
    except pl.exceptions.NoDataError as exc:
        logger.info("No Curious data to export.")
        raise NoData from exc

    initial = formatter_init.produce(ml_data)
    metadata = _fetch_redcap_metadata_for_fields(
        pid,
        _extract_field_names_from_outputs(initial),
    )
    fmt = RedcapImportFormat(project=event_names, redcap_metadata=metadata)
    outputs = fmt.produce(ml_data) if metadata is not None else initial
    logger.info(
        "Formatted instruments: %s",
        "".join(f"\n\t- {o.name[:-7]}" for o in outputs),
    )
    return outputs, fmt.get_instrument_row_counts()


def get_curious_data(request_json: CliOptions) -> None:
    """Pull Curious data via the JS auto-export job."""
    tsx(
        Config.PROJECT_ROOT / "javascript_jobs/autoexport/src/index.ts",
        request_json.long.split(" "),
        parse_output=False,
    )


# ============================================================================
# MRN Validation
# ============================================================================


def _validate_csv_structure(df: pl.DataFrame, csv_name: str) -> tuple[bool, list[str]]:
    """Return ``(is_valid, data_fields)``."""
    if "record_id" not in df.columns:
        return False, []
    data_fields = [c for c in df.columns if c not in STANDARD_FIELDS]
    return (True, data_fields) if data_fields else (False, [])


def validate_and_map_mrns(csv_path: Path, pid: int) -> bool:
    """Validate and map MRNs to record IDs if needed."""
    df_pl = pl.read_csv(csv_path)
    ok, data_fields = _validate_csv_structure(df_pl, csv_path.name)
    if not ok:
        return False
    logger.info("Validating MRN mapping for %s", csv_path.name)
    df_pd = df_pl.to_pandas()
    rows = [
        {
            "record": str(row["record_id"]),
            "field_name": field,
            "value": str(row[field]) if pd.notna(row[field]) else "",
            "redcap_event_name": row.get("redcap_event_name", ""),
        }
        for _, row in df_pd.head(10).iterrows()
        for field in data_fields[:5]
    ]
    if rows:
        try:
            redcap_fields = fetch_data(
                get_redcap_token(pid),
                str(FieldList(list({*data_fields[:10], "mrn"}))),
                all_or_any="any",
            )
            if redcap_fields.empty:
                return False
            _, mrn_lookup = map_mrns_to_records(pd.DataFrame(rows), redcap_fields)
            if not mrn_lookup:
                return False
            original = set(df_pd["record_id"].astype(str).unique())
            mappable = original & set(mrn_lookup.keys())
            if not mappable:
                return False
            df_pd["record_id"] = (
                df_pd["record_id"]
                .astype(str)
                .map(
                    lambda x: mrn_lookup.get(str(x), x),
                )
            )
            pl.from_pandas(df_pd).write_csv(csv_path)
            logger.info(
                "Mapped %d MRNs in %s (%d total)",
                len(mappable),
                csv_path.name,
                len(original),
            )
            return True
        except Exception as e:
            logger.warning("MRN mapping error for %s: %s", csv_path.name, e)
    return False


# ============================================================================
# Alert Field Management
# ============================================================================


def _determine_column(df: pl.DataFrame, candidates: tuple[str, ...]) -> str | None:
    """Return the first column from *candidates* present in *df*."""
    return next((c for c in candidates if c in df.columns), None)


def add_alert_fields_if_needed(csv_path: Path, pid: int) -> None:
    """Add alert field ``'no'`` where it isn't already ``'yes'``."""
    df = pl.read_csv(csv_path)
    inst = csv_path.stem.lower()
    if inst not in [
        i.lower() for i in possible_alert_instruments(REDCAP_ENDPOINTS.base_url, pid)
    ]:
        return
    logger.info("Processing alerts for %s", inst)
    rid_col = _determine_column(
        df, ("record_id", "record", "participant_id", "subject_id")
    )
    if not rid_col:
        logger.warning("No record ID column in %s", inst)
        return
    evt_col = _determine_column(df, ("redcap_event_name", "event"))
    alert_field = f"{inst}_alerts"
    if alert_field in df.columns:
        return

    records_str = [str(r) for r in df[rid_col].unique().to_list()]
    logger.info("Waiting 15 s for websocket alerts (%d records)…", len(records_str))
    time.sleep(15)

    alert_event = get_alert_field_event(REDCAP_ENDPOINTS.base_url, pid, inst)
    if not alert_event:
        return
    existing = get_redcap_records_for_instrument(inst, records_str, pid, alert_event)

    alert_rows: list[dict] = []
    yes_count = 0
    for rid in df[rid_col].unique():
        if existing.get(str(rid), {}).get(alert_field, "").strip() == "yes":
            yes_count += 1
        else:
            row: dict = {rid_col: rid, alert_field: "no"}
            if evt_col:
                row[evt_col] = alert_event
            alert_rows.append(row)
    logger.info("Alerts for %s: %d 'yes', %d → 'no'", inst, yes_count, len(alert_rows))
    if not alert_rows:
        return
    alert_df = pl.DataFrame(alert_rows)
    if not evt_col:
        alert_df = alert_df.with_columns(pl.lit(alert_event).alias("redcap_event_name"))
    pl.concat([df, alert_df], how="diagonal").write_csv(csv_path)


# ============================================================================
# Error Handling & Validation
# ============================================================================


def extract_unfound_fields(error_text: str) -> list[str]:
    """Extract field names from a REDCap "fields not found" error."""
    m = re.search(
        r"The following fields were not found in the project as real data fields:"
        r" (.+?)(?:\n|$)",
        error_text,
    )
    return [f.strip() for f in m.group(1).split(",")] if m else []


def extract_invalid_category_errors(error_text: str) -> list[dict[str, str]]:
    """Extract invalid-category errors from REDCap error text."""
    pattern = (
        r'"([^"]+)","([^"]+)","([^"]*)","The value is not a valid category for ([^"]+)"'
    )
    return [
        {
            "record": m.group(1),
            "field_name": m.group(2),
            "value": m.group(3),
            "error_message": f"The value is not a valid category for {m.group(4)}",
        }
        for m in re.finditer(pattern, error_text)
    ]


def save_invalid_category_errors(csv_path: Path, errors: list[dict[str, str]]) -> Path:
    """Save invalid-category errors to a timestamped log CSV."""
    log_dir = Config.LOG_ROOT / "invalid_categories"
    log_dir.mkdir(parents=True, exist_ok=True)
    out = (
        log_dir
        / f"{csv_path.stem}_invalid_categories_{datetime.now():%Y%m%d_%H%M%S}.csv"
    )
    with out.open("w", newline="") as f:
        if errors:
            w = csv.DictWriter(
                f, fieldnames=["record", "field_name", "value", "error_message"]
            )
            w.writeheader()
            w.writerows(errors)
    return out


def split_csv_by_fields(
    csv_path: Path, unfound_fields: list[str]
) -> tuple[Path, Path | None]:
    """Split CSV into valid-field and unfound-field files."""
    df = pl.read_csv(csv_path)
    id_cols = [c for c in STANDARD_FIELDS if c in df.columns]
    unfound = [c for c in unfound_fields if c in df.columns]
    if not unfound:
        return csv_path, None
    valid_cols = [c for c in df.columns if c not in unfound]
    valid_path = csv_path.with_stem(f"{csv_path.stem}_valid_fields")
    df.select(valid_cols).write_csv(valid_path)
    log_dir = Config.LOG_ROOT / "unfound_fields"
    log_dir.mkdir(parents=True, exist_ok=True)
    uf_path = (
        log_dir / f"{csv_path.stem}_unfound_fields_{datetime.now():%Y%m%d_%H%M%S}.csv"
    )
    df.select(id_cols + unfound).write_csv(uf_path)
    logger.info(
        "Split %s → valid (%d cols): %s, unfound (%d cols): %s",
        csv_path.name,
        len(valid_cols),
        valid_path.name,
        len(unfound),
        uf_path,
    )
    return valid_path, uf_path


def validate_fields_against_metadata(
    df: pd.DataFrame,
    metadata: pd.DataFrame,
    instrument_name: str,
) -> tuple[list[str], list[str]]:
    """Return ``(valid_fields, invalid_fields)`` for *df* columns."""
    known: set[str] = {
        *metadata["field_name"].tolist(),
        f"{instrument_name.lower()}_complete",
    }
    df_fields = set(df.columns) - {"record_id", "redcap_event_name"}
    invalid = [f for f in df_fields if f not in known]
    valid = [f for f in df_fields if f in known]
    if invalid:
        logger.warning(
            "%d invalid fields in %s: %s",
            len(invalid),
            instrument_name,
            ", ".join(invalid),
        )
    return valid, invalid


def chunk_dataframe_by_columns(
    df: pl.DataFrame,
    max_columns: int = 100,
    required_columns: list[str] | None = None,
) -> list[pl.DataFrame]:
    """Split *df* into column-chunks to avoid API timeouts."""
    req = [
        c
        for c in (required_columns or ["record_id", "redcap_event_name"])
        if c in df.columns
    ]
    data = [c for c in df.columns if c not in req]
    if len(df.columns) <= max_columns + len(req):
        return [df]
    return [
        df.select(req + data[i : i + max_columns])
        for i in range(0, len(data), max_columns)
    ]


# ============================================================================
# REDCap Upload
# ============================================================================


def _upload_csv_to_redcap(
    csv_path: Path, pid: int, retry_on_field_error: bool = True
) -> None:
    """Upload a single CSV file to REDCap."""
    payload = {
        "token": get_redcap_token(pid),
        "content": "record",
        "action": "import",
        "format": "csv",
        "type": "flat",
        "overwriteBehavior": "normal",
        "forceAutoNumber": "false",
        "data": csv_path.read_text(),
        "returnContent": "count",
        "returnFormat": "csv",
    }
    r = requests.post(REDCAP_ENDPOINTS.base_url, data=payload, timeout=180)
    if r.status_code == requests.codes["okay"]:
        return
    logger.exception("Bad Request\n%s\nHTTP %d", r.text, r.status_code)
    cat_errors = extract_invalid_category_errors(r.text)
    if cat_errors:
        logger.warning(
            "%d invalid category errors → %s",
            len(cat_errors),
            save_invalid_category_errors(csv_path, cat_errors),
        )
        r.raise_for_status()
    if retry_on_field_error and "fields were not found in the project" in r.text:
        unfound = extract_unfound_fields(r.text)
        if unfound:
            logger.warning("%d unfound fields: %s", len(unfound), ", ".join(unfound))
            valid_path, uf = split_csv_by_fields(csv_path, unfound)
            if uf:
                logger.warning("Unfound fields → %s", uf)
            if valid_path != csv_path:
                _upload_csv_to_redcap(valid_path, pid, retry_on_field_error=False)
                return
    r.raise_for_status()


def _validate_and_filter_fields(
    df: pl.DataFrame, pid: int, instrument_name: str
) -> pl.DataFrame:
    """Pre-upload: remove fields not in REDCap metadata."""
    try:
        md = fetch_api_data(
            REDCAP_ENDPOINTS.base_url,
            redcap_variables.headers,
            {"token": get_redcap_token(pid), "content": "metadata", "format": "csv"},
        )
        if md.empty:
            return df
        valid, invalid = validate_fields_against_metadata(
            df.to_pandas(), md, instrument_name
        )
        if not invalid:
            return df
        req = [c for c in STANDARD_FIELDS if c in df.columns]
        keep = req + [f for f in valid if f in df.columns and f not in req]
        logger.info(
            "Proceeding with %d valid cols (removed %d)", len(keep), len(invalid)
        )
        return df.select(keep)
    except Exception as e:
        logger.warning("Pre-upload validation failed: %s", e)
        return df


def _upload_chunked(df: pl.DataFrame, csv_path: Path, pid: int, retry: bool) -> None:
    """Upload *df* in column-chunks."""
    req = [c for c in STANDARD_FIELDS if c in df.columns]
    chunks = chunk_dataframe_by_columns(df, max_columns=100, required_columns=req)
    logger.info("Split into %d chunks", len(chunks))
    for i, chunk in enumerate(chunks, 1):
        cp = csv_path.with_stem(f"{csv_path.stem}_chunk_{i}")
        chunk.write_csv(cp)
        logger.info(
            "Uploading chunk %d/%d (%d cols)…", i, len(chunks), len(chunk.columns)
        )
        try:
            _upload_csv_to_redcap(cp, pid, retry)
        finally:
            if cp.exists():
                cp.unlink()


def push_to_redcap(
    csv_path: Path,
    pid: int,
    retry_on_field_error: bool = True,
    skip_deduplication: bool = False,
) -> None:
    """Push data to REDCap with preprocessing, validation, and dedup."""
    if not csv_path.stat().st_size:
        logger.info("Skipping empty file: %s", csv_path)
        return
    validate_and_map_mrns(csv_path, pid)
    if pid == 625:  # noqa: PLR2004
        add_alert_fields_if_needed(csv_path, pid)

    df = pl.read_csv(csv_path)
    df = _validate_and_filter_fields(df, pid, csv_path.stem)

    if not skip_deduplication:
        df, _ = deduplicate_dataframe(
            df,
            get_redcap_token(pid),
            REDCAP_ENDPOINTS.base_url,
            redcap_variables.headers,
            csv_path.stem,
        )
        if df.is_empty():
            logger.info("All rows in %s are duplicates, skipping", csv_path.name)
            return

    if len(df.columns) > Config.COLUMN_CHUNK_SIZE:
        _upload_chunked(df, csv_path, pid, retry_on_field_error)
    else:
        df.write_csv(csv_path)
        _upload_csv_to_redcap(csv_path, pid, retry_on_field_error)


def save_for_redcap(outputs: list[NamedOutput], redcap_data_dir: Path) -> None:
    """Save formatted outputs as CSV files."""
    for output in outputs:
        output.output.write_csv(
            (redcap_data_dir / output.name.replace("_redcap", "")).with_suffix(".csv"),
        )


def send_to_redcap(
    redcap_path: Path,
    pid: int,
    instrument_row_count: dict[str, int],
    cache: DataCache | None = None,
) -> Results:
    """Send CSV files under *redcap_path* to REDCap *pid*."""
    results = Results()
    instruments: list[str] = [
        i.lower()
        for i in fetch_api_data(
            REDCAP_ENDPOINTS.base_url,
            redcap_variables.headers,
            {
                "token": get_redcap_token(pid),
                "content": "instrument",
                "format": "csv",
                "returnFormat": "csv",
            },
        )["instrument_name"].tolist()
    ]
    to_send = [f for f in redcap_path.iterdir() if f.stem.lower() in instruments]
    logger.info(
        "Ready to send to PID %d: %s", pid, "".join(f"\n\t- {f.stem}" for f in to_send)
    )
    for path in to_send:
        key = path.stem
        rows = instrument_row_count.get(key, 0)
        cache_key, file_hash = key, ""
        if cache:
            file_hash = compute_dataframe_hash(pl.read_csv(path))
            cache_key = create_instrument_cache_key(key, file_hash, rows)
            if cache.is_processed(cache_key):
                logger.info("Skipping %s (cached)", key)
                results.success += rows
                continue
        try:
            push_to_redcap(path, pid)
            results.success += rows
            if cache:
                cache.mark_processed(
                    cache_key,
                    metadata={
                        "instrument": key,
                        "row_count": rows,
                        "file_hash": file_hash,
                    },
                )
        except Exception:
            logger.exception("%s\n", path)
            results.failure.append(key)
    return results


def _send_received_flags(
    received_df: pl.DataFrame,
    pid: int,
    cache: DataCache | None = None,
) -> Results:
    """Upload ``_received`` flag rows to PID 625."""
    results = Results()
    if received_df.is_empty():
        return results
    with NamedTemporaryFile(suffix=".csv", delete=False, mode="w") as tmp:
        tmp_path = Path(tmp.name)
        received_df.write_csv(tmp_path)
    try:
        fh = compute_dataframe_hash(received_df)
        ck = create_instrument_cache_key("received_flags", fh, len(received_df))
        if cache and cache.is_processed(ck):
            logger.info("Received flags already processed, skipping")
            results.success += len(received_df)
            return results
        push_to_redcap(tmp_path, pid, skip_deduplication=True)
        results.success += len(received_df)
        if cache:
            cache.mark_processed(
                ck, metadata={"type": "received_flags", "rows": len(received_df)}
            )
    except Exception:
        logger.exception("Failed to upload received flags to PID %d", pid)
        results.failure.append("received_flags")
    finally:
        if tmp_path.exists():
            tmp_path.unlink()
    return results


# ============================================================================
# Pipeline Orchestration
# ============================================================================


def _export_curious_data(
    applet_credentials: dict[str, str],
    data_dir_paths: dict[str, Path],
    request_json: CliOptions,
) -> Path:
    """Export data from Curious; return path to the export file."""
    os.environ.update({k.upper(): v for k, v in applet_credentials.items()})
    export_file = data_dir_paths["curious"] / "responses_curious.csv"
    request_json["output"] = str(export_file)
    get_curious_data(request_json)
    return export_file


def data_to_redcap(
    applet_name: str, request_json: CliOptions, cache: DataCache
) -> None:
    """
    Send Curious data to REDCap for *applet_name*.

    * **PID 625** — ``curious_account_created``, alerts, ``_received`` flags.
    * **PID 891** — all other instrument data.
    """
    with TemporaryDirectory() as tmpdir:
        root = Path(tmpdir)
        dirs = {src: root / f"from_{src}" for src in ("curious", "redcap")}
        for d in dirs.values():
            d.mkdir(parents=True, exist_ok=True)

        _export_curious_data(APPLET_CREDENTIALS[applet_name], dirs, request_json)

        try:
            outputs, _irc = format_for_redcap(891, dirs["curious"])
        except NoData:
            return
        irc: dict[str, int] = {k: v for k, v in _irc.items() if v is not None}
        by_pid = _split_outputs_by_pid(outputs, irc)

        # PID 891 — questionnaire data
        if by_pid[891][0]:
            d891 = root / "redcap_891"
            d891.mkdir(parents=True, exist_ok=True)
            save_for_redcap(by_pid[891][0], d891)
            logger.info(
                send_to_redcap(d891, 891, by_pid[891][1], cache).report, YESTERDAY
            )

        # PID 625 — account-created instruments
        if by_pid[625][0]:
            d625 = root / "redcap_625"
            d625.mkdir(parents=True, exist_ok=True)
            save_for_redcap(by_pid[625][0], d625)
            logger.info(
                send_to_redcap(d625, 625, by_pid[625][1], cache).report, YESTERDAY
            )

        # PID 625 — received flags
        received = _build_received_flags_df(outputs)
        if received is not None:
            rr = _send_received_flags(received, 625, cache)
            logger.info("Received flags: %d ok, %d fail", rr.success, len(rr.failure))

        log_cache_statistics(cache, logger)


def main() -> None:
    """Send Curious data to REDCap."""
    from_date, to_date = get_recent_time_window(
        minutes_back=2, allow_full_day_fallback=True
    )
    request_json = CliOptions({"fromDate": from_date, "toDate": to_date})
    cache = DataCache("curious_data_to_redcap", ttl_minutes=2)
    exceptions = False
    for project in curious_variables.applets.keys():
        try:
            logger.info(
                "\n=====\nTransferring %s from Curious to REDCap.\n=====\n", project
            )
            data_to_redcap(project, request_json, cache)
        except NoData:
            logger.info("No data available for %s", project)
        except Exception:
            logger.exception("Failed to transfer %s from Curious to REDCap.", project)
            exceptions = True
    sys.exit(bool(exceptions))


if __name__ == "__main__":
    main()
