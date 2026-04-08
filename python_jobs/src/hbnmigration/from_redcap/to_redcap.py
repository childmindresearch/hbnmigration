"""
Transfer data from REDCap to REDCap.

For each subject in PID 247, if `intake_ready` == 1:
- push subject to PID 744 / 625, &
- set `indake_ready` = 2 in PID 247.
"""

import pandas as pd

from .._config_variables import redcap_variables
from ..config import Config
from ..exceptions import NoData
from ..utility_functions import DataCache, initialize_logging, redcap_api_push
from .config import Fields, Values
from .from_redcap import fetch_data

Endpoints = redcap_variables.Endpoints()
logger = initialize_logging(__name__)


def update_source(df: pd.DataFrame, record_ids: dict[int | str, int | str]) -> int:
    """
    Update `intake_ready` column in source project.

    Parameters
    ----------
    df
        destination DataFrame

    record_ids
        mapping of record_ids between two REDCap projects

    Returns
    -------
    int
        number of records updated

    """
    df_274 = pd.DataFrame(
        {
            "record": df["record"].unique(),
            "field_name": "intake_ready",
            "value": Values.PID247.intake_ready[
                "Participant information already sent to HBN - Intake Redcap project"
            ],
        }
    )
    df_274["record"] = df_274["record"].replace({v: k for k, v in record_ids.items()})
    return redcap_api_push(
        df=df_274,
        token=redcap_variables.Tokens.pid247,
        url=Endpoints.base_url,
        headers=redcap_variables.headers,
    )


def update_complete_parent_second_guardian_consent(df: pd.DataFrame) -> pd.DataFrame:
    """
    Update `"parent_second_guardian_consent_complete"` based on `"guardian2_consent"`.

    Only records whose `"guardian2_consent"` value is in `mapping` are affected.
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
    # compute desired target value per record
    record_to_value = (
        df.query("field_name == 'guardian2_consent'")
        .set_index("record")["value"]
        .map(mapping)
        .dropna()
    )
    if record_to_value.empty:
        return df
    records_to_update = record_to_value.index

    # update existing rows
    mask = (df["field_name"] == "complete_parent_second_guardian_consent") & (
        df["record"].isin(records_to_update)
    )
    df.loc[mask, "value"] = df.loc[mask, "record"].map(record_to_value)

    # append missing rows
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
    df = df.sort_values(["record", "field_name"], kind="stable").reset_index(drop=True)
    return df.sort_values(["record", "field_name"], kind="stable").reset_index(
        drop=True
    )


def main() -> None:
    """Transfer data from REDCap to REDCap."""
    # Initialize cache for minute-by-minute transfers (TTL: 2 minutes)
    cache = DataCache("redcap_to_redcap", ttl_minutes=2)

    try:
        # get data from PID247
        data247 = fetch_data(
            redcap_variables.Tokens.pid247,
            str(Fields.export_247.for_redcap_operations),
            Values.PID247.intake_ready.filter_logic("Ready to Send to Intake Redcap"),
        )
        data247["field_name"] = data247["field_name"].replace(
            Fields.rename.redcap_consent_to_redcap_operations
        )
        if data247.empty:
            raise NoData

        # Filter out records already processed by cache
        unique_records = data247["record"].unique()
        unprocessed_records = cache.get_unprocessed_records(unique_records.tolist())

        if len(unprocessed_records) < len(unique_records):
            logger.info(
                "Skipping %d already-processed records (cache hit)",
                len(unique_records) - len(unprocessed_records),
            )
            data247 = data247[data247["record"].isin(unprocessed_records)]

        if data247.empty:
            logger.info("All records already processed in cache.")
            return

        # rename columns for consent project
        data247 = update_complete_parent_second_guardian_consent(data247)
        data247["field_name"] = data247["field_name"].replace(
            Fields.rename.redcap_consent_to_redcap_operations
        )
        # format DataFrame for operations project
        df_operations = data247.loc[
            data247["field_name"].str.startswith(tuple(Fields.import_625))
        ]
        record_ids: dict[int | str, int | str] = {
            row["record"]: row["value"]
            for _, row in df_operations[df_operations["field_name"] == "mrn"].iterrows()
        }
        df_operations["record"] = df_operations["record"].replace(record_ids)
        df_operations.loc[df_operations["field_name"] == "record_id", "value"] = (
            df_operations.loc[df_operations["field_name"] == "record_id", "record"]
        )
        assert isinstance(df_operations, pd.DataFrame)
        df_operations = (
            df_operations.sort_values("redcap_repeat_instance", ascending=False)
            .drop_duplicates(subset=["record", "field_name"], keep="first")
            .drop(columns=["redcap_repeat_instrument", "redcap_repeat_instance"])
            .reset_index(drop=True)
        )
        decrement_mask = df_operations["field_name"] == "permission_collab"
        # Convert to numeric and decrement
        decremented = (
            pd.to_numeric(df_operations.loc[decrement_mask, "value"], errors="coerce")
            - 1
        )
        assert isinstance(decremented, pd.Series)
        # Convert back to string
        df_operations.loc[decrement_mask, "value"] = decremented.astype(str)
        rows_imported_operations = redcap_api_push(
            df=df_operations,
            token=getattr(
                redcap_variables.Tokens,
                "pid625" if Config.PROJECT_STATUS == "prod" else "pid744",
            ),
            url=Endpoints.base_url,
            headers=redcap_variables.headers,
        )
        if not rows_imported_operations:
            raise NoData

        # Mark source records as processed in cache
        source_records = data247["record"].unique().tolist()
        cache.bulk_mark_processed(
            source_records,
            metadata={"rows_imported": rows_imported_operations},
        )

        rows_updated_274 = update_source(df_operations, record_ids)
        assert rows_imported_operations == rows_updated_274, (
            f"rows imported to REDCap operations ({rows_imported_operations}) "
            f"≠ rows updated in REDCap consent ({rows_updated_274})."
        )

        # Log cache statistics
        cache_stats = cache.get_stats()
        logger.info(
            "Cache statistics: %d entries, file size: %d bytes, last activity: %s",
            cache_stats["total_entries"],
            cache_stats["file_size_bytes"],
            cache_stats.get("last_activity", "never"),
        )
    except NoData:
        logger.info(
            "No data to transfer from REDCap consent project to "
            "REDCap operations project."
        )


if __name__ == "__main__":
    main()
