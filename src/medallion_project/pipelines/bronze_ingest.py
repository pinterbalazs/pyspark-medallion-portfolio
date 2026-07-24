from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from medallion_project.bronze.ingest import add_lineage, read_raw, write_delta
from medallion_project.common.manifest import load_manifest
from medallion_project.common.spark import create_spark_session, stop_spark_session


@dataclass(frozen=True)
class TableLoadResult:
    name: str
    path: str
    status: str
    record_count: int | None = None
    error: str | None = None


def _generate_batch_id() -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{timestamp}_{uuid4().hex[:8]}"


class BronzeIngestionPipeline:
    def __init__(
        self,
        manifest_path: str | Path,
        app_name: str = "bronze-ingest-run",
        batch_id: str | None = None,
        mode: str = "overwrite",
        tables: list[str] | None = None,
    ) -> None:
        self.manifest_path = manifest_path
        self.app_name = app_name
        self.batch_id = batch_id or _generate_batch_id()
        self.mode = mode
        self.tables = tables

    def run(self) -> list[TableLoadResult]:
        manifest = load_manifest(self.manifest_path)

        specs = manifest.tables
        if self.tables is not None:
            wanted = set(self.tables)
            specs = tuple(spec for spec in specs if spec.name in wanted)

        spark = create_spark_session(self.app_name)
        results: list[TableLoadResult] = []

        try:
            for spec in specs:
                source = manifest.source_path(spec)
                target = manifest.target_path(spec)

                try:
                    df = read_raw(spark, source, dict(spec.options), spec.fmt)
                    df = add_lineage(df, self.batch_id)
                    write_delta(df, target, self.mode)
                    record_count = df.count()

                    results.append(
                        TableLoadResult(
                            name=spec.name,
                            path=target,
                            status="success",
                            record_count=record_count,
                        )
                    )
                except Exception as exc:  # noqa: BLE001 - isolate per-table failure
                    results.append(
                        TableLoadResult(
                            name=spec.name,
                            path=target,
                            status="failed",
                            error=str(exc),
                        )
                    )
        finally:
            stop_spark_session(spark)

        self._print_summary(results)
        return results

    def _print_summary(self, results: list[TableLoadResult]) -> None:
        succeeded = sum(1 for result in results if result.status == "success")

        print("Bronze ingestion summary")
        print(f"batch_id: {self.batch_id}")
        for result in results:
            if result.status == "success":
                print(
                    f"  {result.name:<32} {result.status:<8} "
                    f"{result.record_count:>10}  -> {result.path}"
                )
            else:
                print(
                    f"  {result.name:<32} {result.status:<8} "
                    f"{'-':>10}  ({result.error})"
                )
        print(f"{succeeded}/{len(results)} tables succeeded.")
