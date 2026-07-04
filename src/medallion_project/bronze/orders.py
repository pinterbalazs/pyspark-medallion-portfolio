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


def write_bronze_orders(
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


def read_bronze_orders(
    spark: SparkSession,
    path: str,
) -> DataFrame:
    return (
        spark.read
        .format("delta")
        .load(path)
    )


def process_bronze_orders(
    spark: SparkSession,
    raw_path: str,
    bronze_path: str,
    batch_id: str,
    mode: str = "overwrite",
) -> DataFrame:
    raw_df = load_raw_orders(spark, raw_path)
    bronze_df = add_bronze_metadata(raw_df, batch_id)

    write_bronze_orders(
        df=bronze_df,
        path=bronze_path,
        mode=mode,
    )

    return read_bronze_orders(spark, bronze_path)
