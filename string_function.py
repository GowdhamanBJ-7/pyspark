import file_path

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark  = SparkSession.builder.appName("string function").getOrCreate()
data = [
    (1, "     Arun   ", "Chennai", "India", 30),
    (2, "Karthi", "Hosur", "India", 23),
    (3, "Kavin", "Salem", "India", 31),
    (4, "Ravi", "Bangalore", "India", 25),
    (5, "kumar", "Bangalore", "India", 25),
    (6, "vijay", "Chennai", "India", 30)
]

df = spark.createDataFrame(data,["id","name","city","country","age"])

df.select(upper(col("name"))).show()
df.select(lower(col("country"))).show()

df.select(length(col("name"))).show()

df.select(trim("name")).show()
df.select(ltrim("name")).show()
df.select(rtrim(col("name"))).show()

df.select(substring(col("name"), 1, 3)).show()  # First 3 characters

df.select(initcap(col("name"))).show()

