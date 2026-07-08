from medallion_project.common.config import LocalConfig
from medallion_project.common.spark import create_spark_session, stop_spark_session
from medallion_project.silver.orders import (
    read_bronze_orders,
    read_silver_orders,
    transform_silver_orders,
    write_silver_orders,
)


class SilverOrdersPipeline:
    def __init__(
        self,
        config: LocalConfig | None = None,
        app_name: str = "silver-orders-run",
        mode: str = "overwrite",
    ) -> None:
        self.config = config or LocalConfig()
        self.app_name = app_name
        self.mode = mode

    def run(self):
        spark = create_spark_session(self.app_name)

        try:
            bronze_df = read_bronze_orders(
                spark=spark,
                path=self.config.bronze_orders_path,
            )

            silver_df = transform_silver_orders(bronze_df)

            write_silver_orders(
                df=silver_df,
                path=self.config.silver_orders_path,
                mode=self.mode,
            )

            result_df = read_silver_orders(
                spark=spark,
                path=self.config.silver_orders_path,
            )

            print("Silver orders Delta table written successfully.")
            print(f"Output path: {self.config.silver_orders_path}")
            print(f"Record count: {result_df.count()}")
            result_df.printSchema()
            result_df.show(truncate=False)

            return result_df

        finally:
            stop_spark_session(spark)