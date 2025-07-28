import file_path
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SQLDemo").getOrCreate()

data = [(1, "Arun", "Chennai"), (2, "Kavin", "Bangalore")]
columns = ["id", "name", "city"]

df = spark.createDataFrame(data, columns)

# Register the DataFrame as a temporary table
df.createOrReplaceTempView("people")

# Now you can use SQL queries!
result = spark.sql("select name, city from people where city = 'Chennai'")
result.show()
