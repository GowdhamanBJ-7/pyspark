import file_path
from pyspark.sql.functions import sum, max, min, avg, round, abs
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("numeric function").getOrCreate()

data = [
    (1,-10001.5),
    (2,-10000.7),
    (3,20000.1),
    (4,50000.6),
    (5,30000.3)
]

column = ["id", "salary"]
df = spark.createDataFrame(data, column)

df.show()

df.select(sum("salary")).show()
df.select(max("salary")).show()

numeric = [sum, max, min, avg, round, abs]

def numeric_function(function_list):
    for current_function in function_list:
            df.select(current_function("salary").alias(f"{current_function}")).show()

numeric_function(numeric)