import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("smoke-test")
    .getOrCreate()
)

df = spark.createDataFrame(
    [
        (1, "Alice"),
        (2, "Bob"),
        (3, "Charlie"),
    ],
    ["id", "name"],
)

df.show()

spark.stop()