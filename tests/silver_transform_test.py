from pyspark.sql import functions as F

from medallion_project.common.spark import create_spark_session, stop_spark_session
from medallion_project.silver.transform import (
    add_silver_metadata,
    add_surrogate_id,
    apply_casts_and_renames,
    compute_row_hash,
    reorder_columns,
)


def test_transform_primitives() -> None:
    spark = create_spark_session("test-silver-transform")

    try:
        df = spark.createDataFrame(
            [("p1", "cat", "10", None), ("p2", "cat", "20", "5")],
            ["product_id", "product_category_name", "product_weight_g", "photos"],
        )

        # --- casts + renames -----------------------------------------------
        typed = apply_casts_and_renames(
            df,
            casts={"product_weight_g": "int"},
            renames={"photos": "product_photos_qty"},
        )
        assert dict(typed.dtypes)["product_weight_g"] == "int"
        assert "product_photos_qty" in typed.columns
        assert "photos" not in typed.columns

        # --- deterministic surrogate id ------------------------------------
        keyed = add_surrogate_id(typed, ["product_id"], with_effective_start=False)
        again = add_surrogate_id(typed, ["product_id"], with_effective_start=False)
        ids_1 = {r["product_id"]: r["id"] for r in keyed.collect()}
        ids_2 = {r["product_id"]: r["id"] for r in again.collect()}
        assert ids_1 == ids_2  # deterministic across calls
        assert len(set(ids_1.values())) == 2  # unique per business key

        # --- SCD2 id versions on effective_start_ts ------------------------
        v1 = add_surrogate_id(
            typed.withColumn("effective_start_ts", F.to_timestamp(F.lit("2026-01-01 00:00:00"))),
            ["product_id"],
            with_effective_start=True,
        )
        v2 = add_surrogate_id(
            typed.withColumn("effective_start_ts", F.to_timestamp(F.lit("2026-02-01 00:00:00"))),
            ["product_id"],
            with_effective_start=True,
        )
        id_v1 = {r["product_id"]: r["id"] for r in v1.collect()}
        id_v2 = {r["product_id"]: r["id"] for r in v2.collect()}
        # Same key + different effective_start_ts -> distinct surrogate (a new version).
        assert id_v1["p1"] != id_v2["p1"]

        # --- null-safe row hash --------------------------------------------
        hashed = compute_row_hash(typed, ["product_category_name", "product_weight_g"])
        hashes = {r["product_id"]: r["row_hash"] for r in hashed.collect()}
        # p1 (weight 10) and p2 (weight 20) differ -> different hashes.
        assert hashes["p1"] != hashes["p2"]
        # A row with a NULL tracked attribute still produces a stable, non-null hash.
        null_df = spark.createDataFrame(
            [("x", None)], "product_id string, product_weight_g string"
        )
        null_hash = compute_row_hash(null_df, ["product_weight_g"]).collect()[0]["row_hash"]
        assert null_hash is not None

        # --- metadata + id-first ordering ----------------------------------
        meta = add_silver_metadata(keyed, "silver.products.scd2", "b1", "2026-01-01 00:00:00")
        ordered = reorder_columns(meta, ["product_id"])
        assert ordered.columns[0] == "id"
        assert ordered.columns[1] == "product_id"
        for column in ("process_name", "silver_batch_id", "silver_loaded_ts"):
            assert column in ordered.columns

    finally:
        stop_spark_session(spark)
