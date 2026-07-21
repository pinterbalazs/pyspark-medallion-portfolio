from datetime import date
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col

from medallion_project.common.config import LocalConfig
from medallion_project.common.spark import create_spark_session, stop_spark_session
from medallion_project.gold.daily_sales import (
    read_gold_daily_sales,
    transform_gold_daily_sales,
)
from medallion_project.pipelines.gold_daily_sales import GoldDailySalesPipeline


def create_silver_orders_df(spark: SparkSession) -> DataFrame:
    rows = [
        (1, "C001", "2024-01-01", "completed", 100.5),
        (2, "C002", "2024-01-01", "completed", 250.0),
        (3, "C001", "2024-01-01", "cancelled", 20.0),
        (4, "C003", "2024-01-02", "cancelled", 80.0),
    ]

    return (
        spark.createDataFrame(
            rows,
            ["order_id", "customer_id", "order_date", "status", "amount"],
        )
        .withColumn("order_date", col("order_date").cast("date"))
    )


def test_transform_gold_daily_sales_aggregation() -> None:
    spark = create_spark_session("test-transform-gold-daily-sales")

    try:
        silver_df = create_silver_orders_df(spark)

        gold_df = transform_gold_daily_sales(silver_df)

        by_key = {
            (row["order_date"], row["status"]): row
            for row in gold_df.collect()
        }

        assert len(by_key) == 3

        completed = by_key[(date(2024, 1, 1), "completed")]
        assert completed["order_count"] == 2
        assert completed["unique_customers"] == 2
        assert completed["total_revenue"] == 350.5
        assert completed["avg_order_value"] == 175.25

        cancelled = by_key[(date(2024, 1, 1), "cancelled")]
        assert cancelled["order_count"] == 1
        assert cancelled["total_revenue"] == 20.0

        assert (date(2024, 1, 2), "cancelled") in by_key

    finally:
        stop_spark_session(spark)


def test_gold_daily_sales_pipeline_writes_delta_table(tmp_path: Path) -> None:
    silver_path = tmp_path / "silver" / "orders"
    gold_path = tmp_path / "gold" / "daily_sales"

    setup_spark = create_spark_session("test-gold-daily-sales-setup")

    try:
        silver_df = create_silver_orders_df(setup_spark)
        (
            silver_df.write
            .format("delta")
            .mode("overwrite")
            .save(str(silver_path))
        )
    finally:
        stop_spark_session(setup_spark)

    config = LocalConfig(
        silver_orders_path=str(silver_path),
        gold_daily_sales_path=str(gold_path),
    )

    pipeline = GoldDailySalesPipeline(
        config=config,
        app_name="test-gold-daily-sales-pipeline",
        mode="overwrite",
    )

    pipeline.run()

    assert (gold_path / "_delta_log").exists()

    spark = create_spark_session("test-gold-daily-sales-verify")

    try:
        read_df = read_gold_daily_sales(
            spark=spark,
            path=str(gold_path),
        )

        expected_columns = {
            "order_date",
            "status",
            "order_count",
            "unique_customers",
            "total_revenue",
            "avg_order_value",
            "gold_processed_timestamp",
        }

        assert read_df.count() == 3
        assert expected_columns.issubset(set(read_df.columns))

    finally:
        stop_spark_session(spark)
