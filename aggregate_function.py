import file_path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg, mean, sum, min, max, count, countDistinct,
    first, last, collect_list, collect_set
)

# Initialize Spark session
spark = SparkSession.builder.appName("Aggregate Functions Example").getOrCreate()

# Sample data
data = [
    ("HR", "Aa", 30000),
    ("HR", "Bb", 40000),
    ("IT", "Dd", 50000),
    ("IT", "Cc", 60000),
    ("IT", "Ee", 50000),
    ("Sales", "Ff", 45000),
    ("Sales", "Gg", 45000),
    ("Sales", "Hh", 45000)
]

# Create DataFrame
df = spark.createDataFrame(data, ["department", "name", "salary"])

# Show original data
print("Original Data:")
df.show()

# Aggregate functions
print("Aggregate Results")
df.groupBy("department").agg(
    mean("salary").alias("mean_salary"),
    avg("salary").alias("avg_salary"),
    sum("salary").alias("total_salary"),
    min("salary").alias("min_salary"),
    max("salary").alias("max_salary"),
    count("salary").alias("salary_count"),
    countDistinct("salary").alias("distinct_salaries"),
    first("name").alias("first_employee"),
    last("name").alias("last_employee"),
    collect_list("name").alias("all_names"),
    collect_set("name").alias("unique_names")
).show(truncate=False)
