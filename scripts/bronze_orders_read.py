from medallion_project.bronze.orders import read_bronze_orders
from medallion_project.common.config import LocalConfig
from medallion_project.common.spark import create_spark_session, stop_spark_session


def main() -> None:
    config = LocalConfig()
    spark = create_spark_session("bronze-orders-read", quiet=True)

    try:
        df = read_bronze_orders(
            spark=spark,
            path=config.bronze_orders_path,
        )

        print("Bronze orders Delta table read successfully.")
        print(f"Input path: {config.bronze_orders_path}")
        print(f"Record count: {df.count()}")

        df.printSchema()
        df.show(truncate=False)

    finally:
        stop_spark_session(spark)

if __name__ == "__main__":
    main()