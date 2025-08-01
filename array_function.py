import file_path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, array,array_contains, size, array_position, array_remove

data = [
    ("Electronics", "phone", 1000),
    ("Electronics", "Laptop", 1500),
    ("Electronics", "Tablet", 800),
    ("Furniture", "Chair", 300),
    ("Furniture", "Table", 300),
    ("Furniture", "Desk", 600)
]
columns = ["category","product","price"]

spark = SparkSession.builder.appName("array_function").getOrCreate()
df = spark.createDataFrame(data, columns)

df = df \
    .withColumn("integrated_col", array("category","product"))\
    .withColumn("check_array_contains", array_contains(col("integrated_col"), "Tablet"))\
    .withColumn("size", size(col("integrated_col"))) \
    .withColumn("pos_of_phone", array_position(col("integrated_col"), "phone")) \
    .withColumn("array_removed", array_remove(col("integrated_col"), "phone"))

df.show(truncate=False)

data = [
    (1, ["login", "view", "click", "logout", "login"]),
    (2, ["view", "purchase", "logout"]),
    (3, ["login", "purchase", "login", "logout"])
]

df = spark.createDataFrame(data, ["user_id", "actions"])
df_remove = df \
    .withColumn("remove_login", array_remove(col("actions"),"login")) \
    .withColumn("purchased_", array_contains(col("actions"),"purchase")) \
    .withColumn("size", size(col("actions")))

df_remove.show()