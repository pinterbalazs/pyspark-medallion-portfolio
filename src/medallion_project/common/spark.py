import os
import sys
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def create_spark_session(
    app_name: str = "medallion-project",
) -> SparkSession:
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    os.environ.setdefault("HADOOP_HOME", r"C:\hadoop")
    os.environ.setdefault("hadoop.home.dir", r"C:\hadoop")

    project_root = Path(__file__).resolve().parents[3]
    spark_tmp_dir = project_root / "tmp" / "spark"
    spark_tmp_dir.mkdir(parents=True, exist_ok=True)

    builder = (
        SparkSession.builder
        .master("local[*]")
        .appName(app_name)
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.local.dir", str(spark_tmp_dir))
        .config("spark.sql.shuffle.partitions", "4")
    )

    return configure_spark_with_delta_pip(builder).getOrCreate()