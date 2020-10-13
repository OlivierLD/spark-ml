#
# Basic validation
#
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("spark") \
    .getOrCreate()

# Spark version number:
print("Spark version: {}".format(spark.version))
spark.stop()

