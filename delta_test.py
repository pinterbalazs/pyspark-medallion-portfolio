from medallion_project.common.spark import create_spark_session

spark = create_spark_session("delta-test")

print("Spark version:", spark.version)

spark.range(5).show()

spark.stop()
