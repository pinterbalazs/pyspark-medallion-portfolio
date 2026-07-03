from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, lit


def load_raw_orders(
    spark: SparkSession,
    path: str,
) -> DataFrame:
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(path)
    )


def add_bronze_metadata(
    df: DataFrame,
    batch_id: str,
) -> DataFrame:
    return (
        df
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_file", input_file_name())
        .withColumn("batch_id", lit(batch_id))
    )