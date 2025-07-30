from pyspark.sql import SparkSession
import file_path

spark = SparkSession.builder.appName("PySpark Filtering Example").getOrCreate()

data = [
    ("Aa", "HR", 5000),
    ("Bb", "IT", 6000),
    ("Cc", "Finance", 7000),
    ("Dd", "IT", 4000),
    ("Ee", "HR", 5500),
    ("Fd", "Finance", 8000),
]
columns = ["name", "department", "salary"]

df = spark.createDataFrame(data, columns)

# Show original data
print("Original Data:")
df.show()

# Filter rows with salary greater than 6000
print("Filter: Salary > 6000")
df.filter(df.salary > 6000).show()

# Filter rows belonging to a specific department
print("Filter: Department = 'IT'")
df.filter(df.department == "IT").show()

# Combine multiple filter conditions
print("Filter: Salary > 5000 and Department = 'HR'")
df.filter((df.salary > 5000) & (df.department == "HR")).show()

# Using SQL-like where clause
print("Filter: Salary < 6000 using where()")
df.where("salary < 6000").show()

# Filter using isin (e.g., department is either IT or HR)
print("Filter: Department in ('IT', 'HR')")
df.filter(df.department.isin("IT", "HR")).show()

# Filter rows where a column is null or not null
from pyspark.sql.functions import col
data_with_nulls = [
    ("Alice", None, 5000),
    ("Bob", "IT", 6000),
    (None, "Finance", 7000),
    ("David", "IT", 4000),
]
df_with_nulls = spark.createDataFrame(data_with_nulls, columns)

print("Filter: Rows where department is not null")
df_with_nulls.filter(col("department").isNotNull()).show()

print("Filter: Rows where name is null")
df_with_nulls.filter(col("name").isNull()).show()

# Stop Spark Session
spark.stop()
