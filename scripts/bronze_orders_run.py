from medallion_project.bronze.orders import process_bronze_orders
from medallion_project.common.config import LocalConfig
from medallion_project.common.spark import create_spark_session, stop_spark_session


def main() -> None:
    config = LocalConfig()
    spark = create_spark_session("bronze-orders-run")

    try:
        df = process_bronze_orders(
            spark=spark,
            raw_path=config.raw_orders_path,
            bronze_path=config.bronze_orders_path,
            batch_id="manual_001",
            mode="overwrite",
        )

        print("Bronze orders Delta table written successfully.")
        print(f"Output path: {config.bronze_orders_path}")
        print(f"Record count: {df.count()}")

        df.printSchema()
        df.show(truncate=False)
    
    finally:
        stop_spark_session(spark)

if __name__ == "__main__":
    main()