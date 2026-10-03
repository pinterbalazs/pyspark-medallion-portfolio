from datetime import datetime, timezone
from typing import Mapping, Sequence
from uuid import uuid4

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# Open (current) SCD2 versions carry this high-date sentinel as effective_end_ts,
# so point-in-time range joins (ts >= start AND ts < end) need no NULL handling.
# Note: kept below year 3000 (not 9999-12-31) because Python's datetime.fromtimestamp
# on Windows raises OSError for year-9999 timestamps when collecting to the driver.
SENTINEL_TS = "2999-12-31 23:59:59"

# Sentinel used inside row_hash so a NULL is distinguishable from an empty string
# and concat_ws does not silently drop it.
_NULL_TOKEN = "<NULL>"


def generate_batch_id() -> str:
    """Composite, dataset-agnostic run id: <utc-timestamp>_<uuid8> (matches bronze)."""
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{timestamp}_{uuid4().hex[:8]}"


def apply_casts_and_renames(
    df: DataFrame,
    casts: Mapping[str, str],
    renames: Mapping[str, str],
) -> DataFrame:
    # Renames first, so casts can be keyed by the final (renamed) column names.
    for src, dst in renames.items():
        if src in df.columns:
            df = df.withColumnRenamed(src, dst)
    for column, dtype in casts.items():
        if column in df.columns:
            df = df.withColumn(column, F.col(column).cast(dtype))
    return df


def compute_row_hash(
    df: DataFrame,
    tracked_attributes: Sequence[str],
) -> DataFrame:
    parts = [
        F.coalesce(F.col(column).cast("string"), F.lit(_NULL_TOKEN))
        for column in tracked_attributes
    ]
    return df.withColumn("row_hash", F.sha2(F.concat_ws("||", *parts), 256))


def add_surrogate_id(
    df: DataFrame,
    business_key: Sequence[str],
    with_effective_start: bool = False,
) -> DataFrame:
    parts = [F.col(column).cast("string") for column in business_key]
    if with_effective_start:
        parts.append(F.col("effective_start_ts").cast("string"))
    return df.withColumn("id", F.sha2(F.concat_ws("||", *parts), 256))


def add_silver_metadata(
    df: DataFrame,
    process_name: str,
    batch_id: str,
    load_ts: str,
) -> DataFrame:
    # Carry the bronze batch id forward under a distinct name (if present).
    if "batch_id" in df.columns:
        df = df.withColumnRenamed("batch_id", "bronze_batch_id")
    return (
        df
        .withColumn("process_name", F.lit(process_name))
        .withColumn("silver_batch_id", F.lit(batch_id))
        .withColumn("silver_loaded_ts", F.to_timestamp(F.lit(load_ts)))
    )


def reorder_columns(
    df: DataFrame,
    business_key: Sequence[str],
) -> DataFrame:
    front = ["id"] + [column for column in business_key if column in df.columns]
    rest = [column for column in df.columns if column not in front]
    return df.select(*front, *rest)
