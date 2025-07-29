import file_path

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Intro").getOrCreate()
sc = spark.sparkContext   # Get the underlying SparkContext
# print(sc.textFile("path"))

data = [("Arun", 25), ("Dinesh", 30)]
df = spark.createDataFrame(data, ["name", "age"])
df.show()

df.select("name").show()

# Filtering Rows
df.filter(df.age == 25).show()

# SQL Syntax
df.createOrReplaceTempView("details")
spark.sql("SELECT name FROM details WHERE age = 30").show()

#Sorting
df.orderBy("name").show()

# Schema Inspection
df.printSchema()

# from pyspark.sql.functions import count
# df.groupBy("name").agg(count("id").alias("num_people")).show()


# "C:\Users\GowdhamanBJ\Downloads\customers-100.csv"
# df_csv = spark.read.csv("C:\\Users\\GowdhamanBJ\\Downloads\\customers-100.csv", header=True, inferSchema=True)
# df_csv.printSchema()
# df_csv.show(5)


