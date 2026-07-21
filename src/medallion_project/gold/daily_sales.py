from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, count, countDistinct, current_timestamp
from pyspark.sql.functions import round as _round
from pyspark.sql.functions import sum as _sum


def read_silver_orders(
    spark: SparkSession,
    path: str,
) -> DataFrame:
    return (
        spark.read
        .format("delta")
        .load(path)
    )


def transform_gold_daily_sales(
    df: DataFrame,
) -> DataFrame:
    return (
        df
        .groupBy("order_date", "status")
        .agg(
            count("order_id").alias("order_count"),
            countDistinct("customer_id").alias("unique_customers"),
            _round(_sum("amount"), 2).alias("total_revenue"),
        )
        .withColumn(
            "avg_order_value",
            _round(col("total_revenue") / col("order_count"), 2),
        )
        .withColumn("gold_processed_timestamp", current_timestamp())
        .orderBy("order_date", "status")
    )


def write_gold_daily_sales(
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


def read_gold_daily_sales(
    spark: SparkSession,
    path: str,
) -> DataFrame:
    return (
        spark.read
        .format("delta")
        .load(path)
    )
