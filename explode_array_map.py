import file_path
from pyspark.sql.functions import explode, explode_outer,posexplode_outer
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("exploded").getOrCreate()

data = [
    (1, ["red", "blue", "green"]),
    (2, ["yellow", "black"]),
    (3, [])  # Empty array
]

df = spark.createDataFrame(data, ["id", "colors"])

print("explode")
df_exploded = df.select("id", explode("colors").alias("color"))
df_exploded.show()

print("explode outer")
df_exploded_outer = df.select("id", explode_outer("colors").alias("exploded_outer"))
df_exploded_outer.show()


df_pos = df.select("id", posexplode_outer("colors").alias("pos", "color"))
df_pos.show()
