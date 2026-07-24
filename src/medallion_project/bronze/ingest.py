from typing import Mapping

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, lit

_BOM = "﻿"


def strip_bom_columns(df: DataFrame) -> DataFrame:
    return df.toDF(*[column.lstrip(_BOM) for column in df.columns])


def read_raw(
    spark: SparkSession,
    path: str,
    options: Mapping[str, str],
    fmt: str = "csv",
) -> DataFrame:
    df = (
        spark.read
        .format(fmt)
        .options(**options)
        .option("inferSchema", "false")
        .load(path)
    )
    return strip_bom_columns(df)


def add_lineage(
    df: DataFrame,
    batch_id: str,
) -> DataFrame:
    return (
        df
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_file", input_file_name())
        .withColumn("batch_id", lit(batch_id))
    )


def write_delta(
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
