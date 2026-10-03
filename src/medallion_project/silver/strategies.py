from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from medallion_project.common.manifest import SilverTableSpec


def _write_initial(df: DataFrame, target_path: str) -> None:
    df.write.format("delta").mode("overwrite").save(target_path)


def _with_null_merge_keys(df: DataFrame, business_key: list[str]) -> DataFrame:
    out = df
    for column in business_key:
        out = out.withColumn(f"__mk_{column}", F.lit(None).cast("string"))
    return out


def _with_merge_keys(df: DataFrame, business_key: list[str]) -> DataFrame:
    out = df
    for column in business_key:
        out = out.withColumn(f"__mk_{column}", F.col(column).cast("string"))
    return out


def scd2_merge(
    spark: SparkSession,
    spec: SilverTableSpec,
    staged: DataFrame,
    target_path: str,
) -> None:
    """Canonical two-branch SCD2 upsert: close changed rows, open new versions, insert new keys.

    Idempotent: unchanged business keys produce no writes because the change set is
    filtered by row_hash inequality against the current target rows.
    """
    business_key = list(spec.business_key)

    if not DeltaTable.isDeltaTable(spark, target_path):
        _write_initial(staged, target_path)
        return

    target = DeltaTable.forPath(spark, target_path)

    current = (
        target.toDF()
        .filter("is_current = true")
        .selectExpr(*business_key, "row_hash as _t_row_hash")
    )

    # Existing keys whose tracked attributes changed (row_hash differs).
    changed = (
        staged.join(current, on=business_key, how="inner")
        .where("row_hash <> _t_row_hash")
        .drop("_t_row_hash")
    )

    insert_cols = staged.columns

    # Branch A: every staged row keyed by the business key -> closes changed current
    # rows (WHEN MATCHED) and inserts brand-new keys (WHEN NOT MATCHED).
    # Branch B: changed rows with NULL merge keys -> never match -> always insert
    # (opens the new current version of a changed key).
    staged_updates = _with_merge_keys(staged, business_key).unionByName(
        _with_null_merge_keys(changed, business_key)
    )

    merge_cond = (
        " AND ".join(f"t.{column} = s.__mk_{column}" for column in business_key)
        + " AND t.is_current = true"
    )

    (
        target.alias("t")
        .merge(staged_updates.alias("s"), merge_cond)
        .whenMatchedUpdate(
            condition="t.row_hash <> s.row_hash",
            set={"is_current": "false", "effective_end_ts": "s.effective_start_ts"},
        )
        .whenNotMatchedInsert(values={column: f"s.{column}" for column in insert_cols})
        .execute()
    )


def append_insert_only(
    spark: SparkSession,
    spec: SilverTableSpec,
    df: DataFrame,
    target_path: str,
) -> None:
    """Insert-only upsert on the business key; re-running an existing batch is a no-op."""
    business_key = list(spec.business_key)

    if not DeltaTable.isDeltaTable(spark, target_path):
        _write_initial(df, target_path)
        return

    target = DeltaTable.forPath(spark, target_path)
    merge_cond = " AND ".join(f"t.{column} = s.{column}" for column in business_key)

    (
        target.alias("t")
        .merge(df.alias("s"), merge_cond)
        .whenNotMatchedInsertAll()
        .execute()
    )


def overwrite_table(df: DataFrame, target_path: str) -> None:
    """Full snapshot / reference overwrite; no history."""
    df.write.format("delta").mode("overwrite").save(target_path)
