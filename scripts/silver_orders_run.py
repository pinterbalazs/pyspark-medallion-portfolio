from medallion_project.common.config import LocalConfig
from medallion_project.common.spark import create_spark_session, stop_spark_session
from medallion_project.silver.orders import process_silver_orders


def main() -> None:
    config = LocalConfig()
    spark = create_spark_session("silver-orders-run")

    try:
        df = process_silver_orders(
            spark=spark,
            bronze_path=config.bronze_orders_path,
            silver_path=config.silver_orders_path,
            mode="overwrite",
        )

        print("Silver orders Delta table written successfully.")
        print(f"Output path: {config.silver_orders_path}")
        print(f"Record count: {df.count()}")

        df.printSchema()
        df.show(truncate=False)

    finally:
        stop_spark_session(spark)


if __name__ == "__main__":
    main()