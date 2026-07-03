from medallion_project.bronze.orders import (
    add_bronze_metadata,
    load_raw_orders,
)
from medallion_project.common.config import LocalConfig
from medallion_project.common.spark import create_spark_session

config = LocalConfig()

spark = create_spark_session("bronze-test")

df = load_raw_orders(
    spark,
    config.raw_orders_path,
)

print("RAW RECORD COUNT:", df.count())

df.printSchema()

bronze_df = add_bronze_metadata(
    df,
    batch_id="local-test-001",
)

bronze_df.show(truncate=False)

spark.stop()