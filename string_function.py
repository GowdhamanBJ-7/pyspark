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

print("upper case")
df.select(upper(col("name"))).show()

print("lower case")
df.select(lower(col("country"))).show()

print("init cap case")
df.select(initcap(col("email"))).show() #first letter of word cap

print("length")
df.select(length(col("name"))).show()

print("trim")
df.select(trim("name")).show()

print("left trim")
df.select(ltrim("name")).show()

print("right trim")
df.select(rtrim(col("name"))).show()

print("substring (position, length)")
df.select(substring(col("name"), 1, 3)).show()  # First 3 characters

print("substring index")
df.select(substring_index(col("email"), "@", 1)).show()  # username part

print("split")
df.select(split(col("email"), "@")).show()

print("repeat (no.of times)")
df.select(repeat(col("name"), 2)).show()

df.select(rpad(col("name"), 10, "*")).show()
df.select(lpad(col("name"), 10, "*")).show()

df.select(regexp_extract(col("email"), '([a-zA-Z0-9._%+-]+)', 1)).show()

df.select(regexp_replace(col("city"), "-", "")).show()
