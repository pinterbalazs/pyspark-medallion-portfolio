from pathlib import Path

from medallion_project.common.spark import create_spark_session, stop_spark_session
from medallion_project.pipelines.silver_ingest import SilverIngestionPipeline


def _write_bronze(spark, path: Path, rows, columns) -> None:
    (
        spark.createDataFrame(rows, columns)
        .write.format("delta")
        .mode("overwrite")
        .save(str(path))
    )


def _write_manifest(path: Path, bronze_dir: Path, silver_dir: Path) -> None:
    path.write_text(
        f"""
[dataset]
name = "test"
bronze_dir = "{bronze_dir.as_posix()}"
silver_dir = "{silver_dir.as_posix()}"

[[tables]]
name = "sellers"
source = "sellers"
strategy = "scd2"
business_key = ["seller_id"]
tracked_attributes = ["seller_zip_code_prefix", "seller_city", "seller_state"]

[[tables]]
name = "order_items"
source = "order_items"
strategy = "append_insert_only"
business_key = ["order_id", "order_item_id"]
[tables.casts]
order_item_id = "int"
price = "double"
freight_value = "double"

[[tables]]
name = "product_category_translation"
source = "product_category_translation"
strategy = "reference_overwrite"
business_key = ["product_category_name"]
""",
        encoding="utf-8",
    )


def test_silver_pipeline_end_to_end(tmp_path: Path) -> None:
    bronze_dir = tmp_path / "bronze"
    silver_dir = tmp_path / "silver"

    setup = create_spark_session("test-silver-pipeline-setup")
    try:
        _write_bronze(
            setup,
            bronze_dir / "sellers",
            [
                ("s1", "01037", "sao paulo", "SP", "b0", "f"),
                ("s2", "13023", "campinas", "SP", "b0", "f"),
            ],
            ["seller_id", "seller_zip_code_prefix", "seller_city", "seller_state", "batch_id", "source_file"],
        )
        _write_bronze(
            setup,
            bronze_dir / "order_items",
            [
                ("o1", "1", "10.5", "2.0", "b0", "f"),
                ("o1", "2", "20.0", "3.0", "b0", "f"),
                ("o2", "1", "30.0", "4.0", "b0", "f"),
            ],
            ["order_id", "order_item_id", "price", "freight_value", "batch_id", "source_file"],
        )
        _write_bronze(
            setup,
            bronze_dir / "product_category_translation",
            [
                ("beleza_saude", "health_beauty", "b0", "f"),
                ("automotivo", "auto", "b0", "f"),
            ],
            ["product_category_name", "product_category_name_english", "batch_id", "source_file"],
        )
    finally:
        stop_spark_session(setup)

    manifest_path = tmp_path / "silver.toml"
    _write_manifest(manifest_path, bronze_dir, silver_dir)

    pipeline = SilverIngestionPipeline(
        manifest_path=manifest_path,
        app_name="test-silver-pipeline",
        batch_id="test_batch_001",
        load_ts="2026-01-01 00:00:00",
    )
    results = pipeline.run()

    by_name = {r.name: r for r in results}
    assert all(r.status == "success" for r in results)
    assert by_name["order_items"].record_count == 3

    for name in ("sellers", "order_items", "product_category_translation"):
        assert (silver_dir / name / "_delta_log").exists()

    verify = create_spark_session("test-silver-pipeline-verify")
    try:
        sellers = verify.read.format("delta").load(str(silver_dir / "sellers"))
        assert sellers.columns[0] == "id"
        assert sellers.count() == 2
        assert all(r["is_current"] for r in sellers.collect())
        assert {r["process_name"] for r in sellers.select("process_name").collect()} == {
            "silver.sellers.scd2"
        }

        order_items = verify.read.format("delta").load(str(silver_dir / "order_items"))
        assert dict(order_items.dtypes)["order_item_id"] == "int"
        assert dict(order_items.dtypes)["price"] == "double"
        assert order_items.count() == 3

        translation = verify.read.format("delta").load(str(silver_dir / "product_category_translation"))
        assert translation.count() == 2
        assert translation.columns[0] == "id"
    finally:
        stop_spark_session(verify)
