from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from medallion_project.common.manifest import SilverTableSpec, load_silver_manifest
from medallion_project.common.spark import create_spark_session, stop_spark_session
from medallion_project.silver.hooks import TRANSFORMS
from medallion_project.silver.strategies import (
    append_insert_only,
    overwrite_table,
    scd2_merge,
)
from medallion_project.silver.transform import (
    SENTINEL_TS,
    add_silver_metadata,
    add_surrogate_id,
    apply_casts_and_renames,
    compute_row_hash,
    generate_batch_id,
    reorder_columns,
)

_OVERWRITE_STRATEGIES = {"reference_overwrite", "snapshot_overwrite"}


@dataclass(frozen=True)
class SilverTableResult:
    name: str
    path: str
    strategy: str
    status: str
    record_count: int | None = None
    error: str | None = None


class SilverIngestionPipeline:
    def __init__(
        self,
        manifest_path: str | Path,
        app_name: str = "silver-ingest-run",
        batch_id: str | None = None,
        load_ts: str | None = None,
        tables: list[str] | None = None,
    ) -> None:
        self.manifest_path = manifest_path
        self.app_name = app_name
        self.batch_id = batch_id or generate_batch_id()
        # One pinned load timestamp per run drives effective_start_ts and the
        # deterministic surrogate id, so re-runs of a batch are reproducible.
        self.load_ts = load_ts or datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        self.tables = tables

    def run(self) -> list[SilverTableResult]:
        manifest = load_silver_manifest(self.manifest_path)

        specs = manifest.tables
        if self.tables is not None:
            wanted = set(self.tables)
            specs = tuple(spec for spec in specs if spec.name in wanted)

        spark = create_spark_session(self.app_name)
        results: list[SilverTableResult] = []

        try:
            for spec in specs:
                source = manifest.source_path(spec)
                target = manifest.target_path(spec)
                try:
                    staged = self._prepare(spark, spec, source)
                    self._dispatch(spark, spec, staged, target)
                    results.append(
                        SilverTableResult(
                            name=spec.name,
                            path=target,
                            strategy=spec.strategy,
                            status="success",
                            record_count=staged.count(),
                        )
                    )
                except Exception as exc:  # noqa: BLE001 - isolate per-table failure
                    results.append(
                        SilverTableResult(
                            name=spec.name,
                            path=target,
                            strategy=spec.strategy,
                            status="failed",
                            error=str(exc),
                        )
                    )
        finally:
            stop_spark_session(spark)

        self._print_summary(results)
        return results

    def _prepare(self, spark, spec: SilverTableSpec, source: str) -> DataFrame:
        process_name = f"silver.{spec.name}.{spec.strategy}"

        df = spark.read.format("delta").load(source)
        df = apply_casts_and_renames(df, spec.casts, spec.renames)

        if spec.transform:
            df = TRANSFORMS[spec.transform](df)

        if spec.strategy == "scd2":
            df = df.withColumn("effective_start_ts", F.to_timestamp(F.lit(self.load_ts)))
            df = compute_row_hash(df, spec.tracked_attributes)
            df = add_surrogate_id(df, spec.business_key, with_effective_start=True)
            df = df.withColumn("effective_end_ts", F.to_timestamp(F.lit(SENTINEL_TS)))
            df = df.withColumn("is_current", F.lit(True))
        else:
            df = add_surrogate_id(df, spec.business_key, with_effective_start=False)

        df = add_silver_metadata(df, process_name, self.batch_id, self.load_ts)
        df = reorder_columns(df, spec.business_key)

        # Guarantee one source row per business key so the MERGE cannot multi-match.
        df = df.dropDuplicates(list(spec.business_key))
        return df

    def _dispatch(self, spark, spec: SilverTableSpec, staged: DataFrame, target: str) -> None:
        if spec.strategy == "scd2":
            scd2_merge(spark, spec, staged, target)
        elif spec.strategy == "append_insert_only":
            append_insert_only(spark, spec, staged, target)
        elif spec.strategy in _OVERWRITE_STRATEGIES:
            overwrite_table(staged, target)
        else:
            raise ValueError(f"Unknown strategy '{spec.strategy}' for table '{spec.name}'.")

    def _print_summary(self, results: list[SilverTableResult]) -> None:
        succeeded = sum(1 for result in results if result.status == "success")

        print("Silver ingestion summary")
        print(f"batch_id: {self.batch_id}  load_ts: {self.load_ts}")
        for result in results:
            count = result.record_count if result.record_count is not None else "-"
            detail = f"-> {result.path}" if result.status == "success" else f"({result.error})"
            print(
                f"  {result.name:<32} {result.strategy:<20} "
                f"{result.status:<8} {count:>10}  {detail}"
            )
        print(f"{succeeded}/{len(results)} tables succeeded.")
