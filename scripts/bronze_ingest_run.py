import argparse
import sys

from medallion_project.pipelines.bronze_ingest import BronzeIngestionPipeline


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run the bronze ingestion pipeline from a TOML manifest.",
    )
    parser.add_argument(
        "--manifest",
        default="conf/bronze/olist.toml",
        help="Path to the ingestion manifest (TOML). Default: conf/bronze/olist.toml",
    )
    parser.add_argument(
        "--mode",
        default="overwrite",
        help="Delta write mode. Default: overwrite",
    )
    parser.add_argument(
        "--batch-id",
        default=None,
        help="Optional explicit batch id (default: generated per run).",
    )
    args = parser.parse_args()

    pipeline = BronzeIngestionPipeline(
        manifest_path=args.manifest,
        batch_id=args.batch_id,
        mode=args.mode,
    )
    results = pipeline.run()

    if any(result.status == "failed" for result in results):
        sys.exit(1)


if __name__ == "__main__":
    main()
