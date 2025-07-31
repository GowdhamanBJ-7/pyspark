import file_path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("cast_function").getOrCreate()

data = [("1","1000.5"),("2","2000.9")]
columns = ["id","salary"]

df = spark.createDataFrame(data, columns)
df.printSchema()

df_casted = df.withColumn("salary_float", col("salary").cast("float"))
df_casted = df_casted.withColumn("salary_int", col("salary_float").cast("int"))
df_casted.printSchema()
df_casted.show()
