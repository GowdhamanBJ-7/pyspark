import file_path

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum,avg, rank, dense_rank, row_number
from pyspark.sql.window import Window
from pyspark.sql import functions as F


spark = SparkSession.builder.appName("Window_function").getOrCreate()

df = spark.read.csv("C:\\Users\\GowdhamanBJ\\Downloads\\products-100.csv",
                               header=True, inferSchema=True)
df.printSchema()
print("Total records ",df.count())

df.show()

df_group = df.groupBy(df["Category"]).agg(sum("price"), avg("price"))
df_group.limit(100).show(truncate=False)


window_spec = Window.partitionBy("Category").orderBy("Internal ID")
df_spec = df.withColumn("rank", rank().over(window_spec))
print("rank function")
df_spec.show(10)

df_spec1 = df_spec.orderBy("rank").groupBy("rank").count()
df_spec1.show()

print("dense_rank")
df_spec = df.withColumn("dense_rank", dense_rank().over(window_spec))
df_spec.show(10)

print("row number")
df_spec = df.withColumn("row number", row_number().over(window_spec))
df_spec.show(10)

print("rank & agg")
df = df \
    .withColumn("rank", rank().over(window_spec)) \
    .withColumn("cumulative_sales", sum("Price").over(window_spec)) \
    .withColumn("average_sales", avg("Price").over(window_spec))
df.show()

#-----

data = [
    ("Electronics", "phone", 1000),
    ("Electronics", "Laptop", 1500),
    ("Electronics", "Tablet", 800),
    ("Furniture", "Chair", 300),
    ("Furniture", "Table", 300),
    ("Furniture", "Desk", 600)
]

df = spark.createDataFrame(data, ["category", "product", "price"])

window_specs = Window.partitionBy("category").orderBy("price")
df_window = df\
    .withColumn("rank", rank().over(window_specs)) \
    .withColumn("dense rank", dense_rank().over(window_specs)) \
    .withColumn("row number", row_number().over(window_specs))

df_window.show()

#-----

data = [
    (1, "2024-01-01", 100,"browsing"),
    (1, "2024-01-02", 200,"browsing"),
    (1, "2024-01-03", 300,"carted"),
    (2, "2024-01-01", 50,"carted"),
    (2, "2024-01-02", 80,"carted"),
    (2, "2024-01-03", 120,"purchased"),
]

spark = SparkSession.builder.appName("LeadLag").getOrCreate()
df = spark.createDataFrame(data, ["customer_id", "date", "amount","status"])

window_spec = Window.partitionBy("customer_id").orderBy("date")

df = df \
    .withColumn("next transaction",  F.lead("amount",1).over(window_spec)) \
    .withColumn("previous transaction", F.lag("amount", 1).over(window_spec))

df.show()

#purchase difference
df = df.withColumn("purchase difference", F.col("amount") - F.lag("amount",1,).over(window_spec))
df.show()

df = df.withColumn("missing value", F.lag("amount",1, 0).over(window_spec))
df.show()

print("status change")
df = df.withColumn("status_change", F.col("status") != F.lag("status", 1).over(window_spec))
df.select("customer_id","amount","status","status_change").show()


