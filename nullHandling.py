from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import mean

import file_path
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,BooleanType

spark = SparkSession.builder.appName("null handling").getOrCreate()

data = [
    (1, "Alice", 30, None, "2025-08-01"),
    (2, None, None, "NY", "2025-08-02"),
    (3, "Charlie", None, None, None),
    (4, "David", 45, "CA", "2025-08-04"),
    (5, None, 29, None, "2025-08-05"),
    (None, "Eva", 35, "TX", None),
    (7, "Frank", None, "FL", "2025-08-07"),
    (8, None, None, None, None),
    (None, None, None, None, None)
]

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name",StringType(),True),
    StructField("age", IntegerType(), True),
    StructField("state", StringType(), True),
    StructField("signup_date", StringType(), True)

])

df = spark.createDataFrame(data=data, schema = schema)
df.cache()
df.show()

print("dropna -- drops the row if their is a single Null element")
df_dropna = df.dropna()
df_dropna.show()

print("drop rows where all columns are null")
df_dropall = df.dropna(how="all")
df_dropall.show()

print("drop if name or id is null")
df_drop = df.dropna(subset=["name","id"])
df_drop.show()

print("filter null columns")
df.filter(F.col("name").isNull()).show()
df.filter(F.col("name").isNotNull()).show()

df.describe().show()
df.select("age").describe().show()

df.filter(F.col("age").isNull()).show()
# mean_age = df.select(F.mean("age")).collect()
mean_age = df.agg(F.mean("age")).collect()[0][0]
print(type(mean_age), mean_age)
df.fillna({"age":mean_age}).show()

df.groupBy("state").count().show()
df.dropna(subset=["id"]).show()

df.fillna("Unknown").show()  # fill all string columns
df.fillna(0).show()  # fill numeric columns
df.fillna({"name":"unknown","age":0}).show() #fill default value