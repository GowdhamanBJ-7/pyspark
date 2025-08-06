from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import file_path
# import file_save_fix

spark = SparkSession.builder.appName("FileWriteExamples").getOrCreate()

schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("city", StringType(), True)
])

data = [
    ("raja", 30, "Bangalore"),
    ("ravi", 25, "Chennai"),
    ("rahul", 28, "Delhi")
]

df = spark.createDataFrame(data, schema)
df.show()

#csv
df.write.mode("append").option("header", True).csv("write_files\sample")
    # .mode("overwrite") \
    # .option("header", True) \
    # .csv("C:/Users/GowdhamanBJ/Desktop/pyspark_output/csv_output")

# csv

# # overwrite
df.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv("C:/Users/GowdhamanBJ/Desktop/pyspark_output/csv_output")
#
# # append (requires same schema; run overwrite first)
df.write \
    .mode("append") \
    .option("header", True) \
    .csv("C:/Users/GowdhamanBJ/Desktop/pyspark_output/csv_output")
#
# ignore (skips if folder exists)
df.write \
    .mode("ignore") \
    .option("header", True).csv("C:/Users/GowdhamanBJ/Desktop/pyspark_output/csv_output")

# error (throws error if folder exists)
df.write.mode("errorifexists").option("header", True)  \
    .csv("C:/Users/GowdhamanBJ/Desktop/pyspark_output/csv_output")

# overwrite
df.write.mode("overwrite").parquet("C:/Users/GowdhamanBJ/Desktop/parquet/overwrite")

# append
df.write.mode("append").parquet("output/parquet/append")

# IGNORE
df.write.mode("ignore").parquet("output/parquet/ignore")

# ERRORIFEXISTS
try:
    df.write.mode("errorifexists").parquet("output/parquet/error")
except Exception as e:
    print("Parquet errorifexists mode:", e)
