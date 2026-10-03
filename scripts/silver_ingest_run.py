import argparse
import sys

from medallion_project.pipelines.silver_ingest import SilverIngestionPipeline


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run the silver ingestion pipeline from a TOML manifest.",
    )
    parser.add_argument(
        "--manifest",
        default="conf/silver/olist.toml",
        help="Path to the silver manifest (TOML). Default: conf/silver/olist.toml",
    )
    parser.add_argument(
        "--batch-id",
        default=None,
        help="Optional explicit batch id (default: generated per run).",
    )
    parser.add_argument(
        "--load-ts",
        default=None,
        help="Optional pinned load timestamp 'YYYY-MM-DD HH:MM:SS' (default: now, UTC).",
    )
    args = parser.parse_args()

    pipeline = SilverIngestionPipeline(
        manifest_path=args.manifest,
        batch_id=args.batch_id,
        load_ts=args.load_ts,
    )
    results = pipeline.run()

    if any(result.status == "failed" for result in results):
        sys.exit(1)


if __name__ == "__main__":
    main()
