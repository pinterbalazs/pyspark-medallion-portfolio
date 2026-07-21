from medallion_project.common.config import LocalConfig
from medallion_project.common.spark import create_spark_session, stop_spark_session
from medallion_project.gold.daily_sales import (
    read_gold_daily_sales,
    read_silver_orders,
    transform_gold_daily_sales,
    write_gold_daily_sales,
)


class GoldDailySalesPipeline:
    def __init__(
        self,
        config: LocalConfig | None = None,
        app_name: str = "gold-daily-sales-run",
        mode: str = "overwrite",
    ) -> None:
        self.config = config or LocalConfig()
        self.app_name = app_name
        self.mode = mode

    def run(self):
        spark = create_spark_session(self.app_name)

        try:
            silver_df = read_silver_orders(
                spark=spark,
                path=self.config.silver_orders_path,
            )

            gold_df = transform_gold_daily_sales(silver_df)

            write_gold_daily_sales(
                df=gold_df,
                path=self.config.gold_daily_sales_path,
                mode=self.mode,
            )

            result_df = read_gold_daily_sales(
                spark=spark,
                path=self.config.gold_daily_sales_path,
            )

            print("Gold daily sales Delta table written successfully.")
            print(f"Output path: {self.config.gold_daily_sales_path}")
            print(f"Record count: {result_df.count()}")
            result_df.printSchema()
            result_df.show(truncate=False)

            return result_df

        finally:
            stop_spark_session(spark)
