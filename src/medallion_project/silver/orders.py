from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp


def read_bronze_orders(
    spark: SparkSession,
    path: str,
) -> DataFrame:
    return (
        spark.read
        .format("delta")
        .load(path)
    )


def transform_silver_orders(
    df: DataFrame,
) -> DataFrame:
    return (
        df
        .filter(col("order_id").isNotNull())
        .filter(col("customer_id").isNotNull())
        .filter(col("order_date").isNotNull())
        .filter(col("status").isNotNull())
        .filter(col("amount").isNotNull())
        .filter(col("amount") >= 0)
        .dropDuplicates(["order_id"])
        .withColumn("silver_processed_timestamp", current_timestamp())
    )


def write_silver_orders(
    df: DataFrame,
    path: str,
    mode: str = "overwrite",
) -> None:
    (
        df.write
        .format("delta")
        .mode(mode)
        .save(path)
    )


def read_silver_orders(
    spark: SparkSession,
    path: str,
) -> DataFrame:
    return (
        spark.read
        .format("delta")
        .load(path)
    )
