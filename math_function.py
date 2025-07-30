from pyspark.sql.functions import abs, ceil, floor, exp, log, pow, sqrt
from pyspark.sql import SparkSession
import file_path

spark = SparkSession.builder.getOrCreate()

data = [(1, 25.3), (2, -9.4), (3, 0.5)]
df = spark.createDataFrame(data, ["id", "value"])

df.withColumn("abs", abs("value")) \
  .withColumn("ceil", ceil("value")) \
  .withColumn("floor", floor("value")) \
  .withColumn("exp", exp("value")) \
  .withColumn("log", log("abs")) \
  .withColumn("power_2", pow("value", 2)) \
  .withColumn("sqrt", sqrt("abs")) \
  .show()
