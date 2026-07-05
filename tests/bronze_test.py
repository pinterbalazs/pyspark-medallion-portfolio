from pathlib import Path

from medallion_project.bronze.orders import (
    add_bronze_metadata,
    load_raw_orders,
    process_bronze_orders,
    read_bronze_orders,
)
from medallion_project.common.spark import create_spark_session, stop_spark_session


def create_test_orders_csv(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    path.write_text(
        "\n".join(
            [
                "order_id,customer_id,order_date,status,amount",
                "1,C001,2024-01-01,completed,100.5",
                "2,C002,2024-01-01,completed,250.0",
                "3,C003,2024-01-02,cancelled,80.0",
            ]
        ),
        encoding="utf-8",
    )


def test_load_raw_orders_count(tmp_path: Path) -> None:
    input_path = tmp_path / "raw" / "orders.csv"
    create_test_orders_csv(input_path)

    spark = create_spark_session("test-load-raw-orders")

    try:
        df = load_raw_orders(
            spark=spark,
            path=str(input_path),
        )

        assert df.count() == 3

    finally:
        stop_spark_session(spark)


def test_add_bronze_metadata_columns(tmp_path: Path) -> None:
    input_path = tmp_path / "raw" / "orders.csv"
    create_test_orders_csv(input_path)

    spark = create_spark_session("test-add-bronze-metadata")

    try:
        raw_df = load_raw_orders(
            spark=spark,
            path=str(input_path),
        )

        bronze_df = add_bronze_metadata(
            df=raw_df,
            batch_id="test_batch_001",
        )

        expected_columns = {
            "ingestion_timestamp",
            "source_file",
            "batch_id",
        }

        assert expected_columns.issubset(set(bronze_df.columns))
        assert bronze_df.count() == 3

    finally:
        stop_spark_session(spark)


def test_process_bronze_orders_writes_delta_table(tmp_path: Path) -> None:
    input_path = tmp_path / "raw" / "orders.csv"
    bronze_path = tmp_path / "bronze" / "orders"

    create_test_orders_csv(input_path)

    spark = create_spark_session("test-process-bronze-orders")

    try:
        df = process_bronze_orders(
            spark=spark,
            raw_path=str(input_path),
            bronze_path=str(bronze_path),
            batch_id="test_batch_001",
            mode="overwrite",
        )

        assert df.count() == 3
        assert (bronze_path / "_delta_log").exists()

        read_df = read_bronze_orders(
            spark=spark,
            path=str(bronze_path),
        )

        expected_columns = {
            "order_id",
            "customer_id",
            "order_date",
            "status",
            "amount",
            "ingestion_timestamp",
            "source_file",
            "batch_id",
        }

        assert read_df.count() == 3
        assert expected_columns.issubset(set(read_df.columns))

    finally:
        stop_spark_session(spark)