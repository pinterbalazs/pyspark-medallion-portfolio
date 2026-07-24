from pathlib import Path

from medallion_project.bronze.ingest import read_raw
from medallion_project.common.spark import create_spark_session, stop_spark_session
from medallion_project.pipelines.bronze_ingest import BronzeIngestionPipeline


def _write_manifest(path: Path, raw_dir: Path, bronze_dir: Path, body: str) -> None:
    header = (
        "[dataset]\n"
        'name = "test"\n'
        f'raw_dir = "{raw_dir.as_posix()}"\n'
        f'bronze_dir = "{bronze_dir.as_posix()}"\n'
        'format = "csv"\n\n'
        "[dataset.default_options]\n"
        "header = true\n"
        'encoding = "UTF-8"\n\n'
    )
    path.write_text(header + body, encoding="utf-8")


def test_read_raw_handles_data_gotchas(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    # (1) Leading-zero zip code prefix.
    customers = raw_dir / "customers.csv"
    customers.write_text(
        "customer_id,customer_zip_code_prefix\nc1,01037\n",
        encoding="utf-8",
    )

    # (2) Free-text field with an embedded newline inside a quoted value.
    reviews = raw_dir / "reviews.csv"
    reviews.write_text(
        'review_id,message\nr1,"line one\nline two"\nr2,"ok"\n',
        encoding="utf-8",
    )

    # (3) File that starts with a UTF-8 BOM (utf-8-sig).
    translation = raw_dir / "translation.csv"
    translation.write_text(
        "product_category_name,product_category_name_english\nbeleza,health\n",
        encoding="utf-8-sig",
    )

    spark = create_spark_session("test-bronze-ingest-gotchas")

    try:
        # (1) all-string read preserves the leading zero.
        customers_df = read_raw(
            spark,
            str(customers),
            {"header": "true"},
        )
        assert dict(customers_df.dtypes)["customer_zip_code_prefix"] == "string"
        assert customers_df.collect()[0]["customer_zip_code_prefix"] == "01037"

        # (2) multiLine keeps the embedded newline row as a single record.
        reviews_df = read_raw(
            spark,
            str(reviews),
            {"header": "true", "multiLine": "true", "escape": '"'},
        )
        assert reviews_df.count() == 2

        # (3) BOM stripped from the first column name.
        translation_df = read_raw(
            spark,
            str(translation),
            {"header": "true"},
        )
        assert all(not c.startswith("﻿") for c in translation_df.columns)
        assert "product_category_name" in translation_df.columns

    finally:
        stop_spark_session(spark)


def test_pipeline_end_to_end(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"
    bronze_dir = tmp_path / "bronze"
    raw_dir.mkdir(parents=True, exist_ok=True)

    (raw_dir / "customers.csv").write_text(
        "customer_id,customer_zip_code_prefix\nc1,01037\nc2,09790\n",
        encoding="utf-8",
    )
    (raw_dir / "sellers.csv").write_text(
        "seller_id,seller_zip_code_prefix\ns1,13023\n",
        encoding="utf-8",
    )

    manifest_path = tmp_path / "test.toml"
    _write_manifest(
        manifest_path,
        raw_dir,
        bronze_dir,
        body=(
            "[[tables]]\n"
            'name = "customers"\n'
            'file = "customers.csv"\n\n'
            "[[tables]]\n"
            'name = "sellers"\n'
            'file = "sellers.csv"\n'
        ),
    )

    pipeline = BronzeIngestionPipeline(
        manifest_path=manifest_path,
        app_name="test-bronze-ingest-e2e",
        batch_id="test_batch_001",
        mode="overwrite",
    )

    results = pipeline.run()

    by_name = {r.name: r for r in results}
    assert by_name["customers"].status == "success"
    assert by_name["customers"].record_count == 2
    assert by_name["sellers"].record_count == 1

    for name in ("customers", "sellers"):
        assert (bronze_dir / name / "_delta_log").exists()

    # Re-open a fresh session to verify lineage on the written Delta table.
    spark = create_spark_session("test-bronze-ingest-e2e-verify")

    try:
        df = spark.read.format("delta").load(str(bronze_dir / "customers"))

        lineage_columns = {"ingestion_timestamp", "source_file", "batch_id"}
        assert lineage_columns.issubset(set(df.columns))

        batch_ids = {row["batch_id"] for row in df.select("batch_id").collect()}
        assert batch_ids == {"test_batch_001"}

    finally:
        stop_spark_session(spark)


def test_pipeline_isolates_table_failures(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"
    bronze_dir = tmp_path / "bronze"
    raw_dir.mkdir(parents=True, exist_ok=True)

    (raw_dir / "customers.csv").write_text(
        "customer_id,customer_zip_code_prefix\nc1,01037\n",
        encoding="utf-8",
    )
    # Note: missing.csv is intentionally NOT created.

    manifest_path = tmp_path / "test.toml"
    _write_manifest(
        manifest_path,
        raw_dir,
        bronze_dir,
        body=(
            "[[tables]]\n"
            'name = "missing"\n'
            'file = "missing.csv"\n\n'
            "[[tables]]\n"
            'name = "customers"\n'
            'file = "customers.csv"\n'
        ),
    )

    pipeline = BronzeIngestionPipeline(
        manifest_path=manifest_path,
        app_name="test-bronze-ingest-isolation",
        batch_id="test_batch_002",
        mode="overwrite",
    )

    results = pipeline.run()
    by_name = {r.name: r for r in results}

    assert by_name["missing"].status == "failed"
    assert by_name["missing"].error is not None
    assert by_name["customers"].status == "success"
    assert by_name["customers"].record_count == 1
    assert (bronze_dir / "customers" / "_delta_log").exists()
