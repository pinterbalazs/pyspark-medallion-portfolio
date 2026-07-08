from medallion_project.bronze.orders import (
    add_bronze_metadata,
    load_raw_orders,
    read_bronze_orders,
    write_bronze_orders,
)
from medallion_project.common.config import LocalConfig
from medallion_project.common.spark import create_spark_session, stop_spark_session


class BronzeOrdersPipeline:
    def __init__(
        self,
        config: LocalConfig | None = None,
        app_name: str = "bronze-orders-run",
        batch_id: str = "manual_001",
        mode: str = "overwrite",
    ) -> None:
        self.config = config or LocalConfig()
        self.app_name = app_name
        self.batch_id = batch_id
        self.mode = mode

    def run(self):
        spark = create_spark_session(self.app_name)

        try:
            raw_df = load_raw_orders(
                spark=spark,
                path=self.config.raw_orders_path,
            )

            bronze_df = add_bronze_metadata(
                df=raw_df,
                batch_id=self.batch_id,
            )

            write_bronze_orders(
                df=bronze_df,
                path=self.config.bronze_orders_path,
                mode=self.mode,
            )

            result_df = read_bronze_orders(
                spark=spark,
                path=self.config.bronze_orders_path,
            )

            print("Bronze orders Delta table written successfully.")
            print(f"Output path: {self.config.bronze_orders_path}")
            print(f"Record count: {result_df.count()}")
            result_df.printSchema()
            result_df.show(truncate=False)

            return result_df

        finally:
            stop_spark_session(spark)