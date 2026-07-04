import contextlib
import os
import sys
from pathlib import Path
from typing import Iterator

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


@contextlib.contextmanager
def suppress_stdout_stderr() -> Iterator:
    """
    Suppress noisy JVM / Ivy / Spark startup and shutdown output.
    This redirects OS-level stdout/stderr, so it also catches messages
    emitted by the JVM, not only Python print/logging output.
    """
    with open(os.devnull, "w") as devnull:
        old_stdout_fd = os.dup(1)
        old_stderr_fd = os.dup(2)

        try:
            os.dup2(devnull.fileno(), 1)
            os.dup2(devnull.fileno(), 2)
            yield
        finally:
            os.dup2(old_stdout_fd, 1)
            os.dup2(old_stderr_fd, 2)
            os.close(old_stdout_fd)
            os.close(old_stderr_fd)


def create_spark_session(
    app_name: str = "medallion-project",
    quiet: bool = True,
) -> SparkSession:
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    hadoop_home = r"C:\hadoop"
    hadoop_bin = r"C:\hadoop\bin"

    os.environ.setdefault("HADOOP_HOME", hadoop_home)
    os.environ.setdefault("hadoop.home.dir", hadoop_home)

    if hadoop_bin not in os.environ["PATH"]:
        os.environ["PATH"] = hadoop_bin + os.pathsep + os.environ["PATH"]

    project_root = Path(__file__).resolve().parents[3]
    spark_tmp_dir = project_root / "tmp" / "spark"
    spark_tmp_dir.mkdir(parents=True, exist_ok=True)

    log4j_config_path = project_root / "conf" / "log4j2.properties"
    log4j_java_option = f"-Dlog4j.configurationFile={log4j_config_path.as_uri()}"

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
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.debug.maxToStringFields", "200")
        .config("spark.driver.extraJavaOptions", log4j_java_option)
        .config("spark.executor.extraJavaOptions", log4j_java_option)
    )

    if quiet:
        with suppress_stdout_stderr():
            spark = configure_spark_with_delta_pip(builder).getOrCreate()
    else:
        spark = configure_spark_with_delta_pip(builder).getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    return spark


def stop_spark_session(
    spark: SparkSession,
    quiet: bool = True,
) -> None:
    if quiet:
        with suppress_stdout_stderr():
            spark.stop()
    else:
        spark.stop()