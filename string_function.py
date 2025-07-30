import file_path

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark  = SparkSession.builder.appName("string function").getOrCreate()
data = [
    (1, "     Arun   ", "Chennai-TN", "India", 30, "arun@gmail.com"),
    (2, "Karthi", "Hosur-tn", "India", 23,"arun@gmail.com"),
    (3, "Kavin", "Salem-tn", "India", 31,"arun@gmail.com"),
    (4, "Ravi", "Bangalore-ka", "India", 25,"arun@gmail.com"),
    (5, "kumar", "Bangalore-ka", "India", 25,"arun@gmail.com"),
    (6, "vijay", "Chennai-tn", "India", 30,"arun@gmail.com")
]

df = spark.createDataFrame(data,["id","name","city","country","age", "email"])

df.select(upper(col("name"))).show()
df.select(lower(col("country"))).show()

df.select(length(col("name"))).show()

df.select(trim("name")).show()
df.select(ltrim("name")).show()
df.select(rtrim(col("name"))).show()

df.select(substring(col("name"), 1, 3)).show()  # First 3 characters

df.select(initcap(col("name"))).show()

df.select(substring_index(col("email"), "@", 1)).show()  # username part

df.select(split(col("email"), "@")).show()

df.select(repeat(col("name"), 2)).show()

df.select(rpad(col("name"), 10, "*")).show()
df.select(lpad(col("name"), 10, "*")).show()

df.select(regexp_extract(col("email"), '([a-zA-Z0-9._%+-]+)', 1)).show()

df.select(regexp_replace(col("city"), "-", "")).show()
