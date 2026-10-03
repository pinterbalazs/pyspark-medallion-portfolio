from datetime import datetime
from pathlib import Path

from pyspark.sql import functions as F

from medallion_project.common.manifest import SilverTableSpec
from medallion_project.common.spark import create_spark_session, stop_spark_session
from medallion_project.silver.strategies import scd2_merge
from medallion_project.silver.transform import (
    SENTINEL_TS,
    add_silver_metadata,
    add_surrogate_id,
    compute_row_hash,
    reorder_columns,
)

SPEC = SilverTableSpec(
    name="products",
    source="products",
    strategy="scd2",
    business_key=("product_id",),
    tracked_attributes=("category",),
)


def build_staged(spark, rows, load_ts, batch_id):
    df = spark.createDataFrame(rows, ["product_id", "category"])
    df = df.withColumn("effective_start_ts", F.to_timestamp(F.lit(load_ts)))
    df = compute_row_hash(df, SPEC.tracked_attributes)
    df = add_surrogate_id(df, SPEC.business_key, with_effective_start=True)
    df = df.withColumn("effective_end_ts", F.to_timestamp(F.lit(SENTINEL_TS)))
    df = df.withColumn("is_current", F.lit(True))
    df = add_silver_metadata(df, "silver.products.scd2", batch_id, load_ts)
    df = reorder_columns(df, SPEC.business_key)
    return df.dropDuplicates(list(SPEC.business_key))


def test_scd2_initial_change_and_idempotent(tmp_path: Path) -> None:
    target = str(tmp_path / "products")
    ts1, ts2, ts3 = (
        "2026-01-01 00:00:00",
        "2026-01-02 00:00:00",
        "2026-01-03 00:00:00",
    )

    spark = create_spark_session("test-silver-scd2")

    try:
        # --- initial load ---------------------------------------------------
        scd2_merge(spark, SPEC, build_staged(spark, [("p1", "A"), ("p2", "B")], ts1, "b1"), target)

        rows = spark.read.format("delta").load(target).collect()
        assert len(rows) == 2
        assert all(r["is_current"] for r in rows)
        assert all(r["effective_end_ts"] == datetime(2999, 12, 31, 23, 59, 59) for r in rows)
        assert rows[0]["id"] is not None and rows[0].__contains__("process_name")

        # --- change (p2 B->C) + new key (p3) --------------------------------
        scd2_merge(
            spark,
            SPEC,
            build_staged(spark, [("p1", "A"), ("p2", "C"), ("p3", "D")], ts2, "b2"),
            target,
        )

        df = spark.read.format("delta").load(target)
        all_rows = df.collect()
        current = [r for r in all_rows if r["is_current"]]
        assert len(all_rows) == 4  # p1(1) + p2(old+new=2) + p3(1)
        assert len(current) == 3   # p1, p2-new, p3

        p2_rows = [r for r in all_rows if r["product_id"] == "p2"]
        assert len(p2_rows) == 2
        p2_closed = [r for r in p2_rows if not r["is_current"]][0]
        p2_current = [r for r in p2_rows if r["is_current"]][0]
        # Old version closed at the new version's start -> contiguous validity.
        assert p2_closed["effective_end_ts"] == datetime(2026, 1, 2, 0, 0, 0)
        assert p2_current["effective_start_ts"] == datetime(2026, 1, 2, 0, 0, 0)
        assert p2_current["category"] == "C"

        # p3 is a brand-new current version.
        p3_rows = [r for r in all_rows if r["product_id"] == "p3"]
        assert len(p3_rows) == 1 and p3_rows[0]["is_current"]

        # --- idempotent re-run (same data as current state) -----------------
        scd2_merge(
            spark,
            SPEC,
            build_staged(spark, [("p1", "A"), ("p2", "C"), ("p3", "D")], ts3, "b3"),
            target,
        )

        after = spark.read.format("delta").load(target).collect()
        assert len(after) == 4  # no new rows
        assert len([r for r in after if r["is_current"]]) == 3

    finally:
        stop_spark_session(spark)
