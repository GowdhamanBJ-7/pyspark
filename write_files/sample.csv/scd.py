from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_date, col, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType
import file_path

spark = SparkSession.builder.appName("SCD_Examples").getOrCreate()

# Explicit schema for dimension table
dim_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("start_date", StringType(), True),  # Use DateType() if converting to date
    StructField("end_date", StringType(), True),
    StructField("is_current", BooleanType(), True),
    StructField("previous_name", StringType(), True)
])
data = [
    (1, "Alice", "alice@example.com", "2024-01-01", None, True, None),
    (2, "Bob", "bob@example.com", "2024-01-01", None, True, None),
    (3, "Charlie", "charlie@example.com", "2024-01-01", None, True, None)
]
schema = ["customer_id", "name", "email", "start_date", "end_date", "is_current", "previous_name"]
# Existing dimension table
dim_df = spark.createDataFrame(data, schema=dim_schema)

# Incoming updated data
data1 = [
    (1, "Alice A", "alice@example.com"),
    (2, "Bob", "bob.new@example.com"),
    (4, "Diana", "diana@example.com")
]
schema1 = ["customer_id", "name", "email"]
incoming_df = spark.createDataFrame(data1, schema1)

#Type 0 - no change
scd0_df = dim_df
scd0_df.show()

#type 1 - No history
scd1_df = dim_df.alias("dim").join(incoming_df.alias("inc"), "customer_id", "outer") \
    .select(
        col("customer_id"),
        when(col("inc.name").isNotNull(), col("inc.name")).otherwise(col("dim.name")).alias("name"),
        when(col("inc.email").isNotNull(), col("inc.email")).otherwise(col("dim.email")).alias("email"),
        col("dim.start_date"),
        col("dim.end_date"),
        col("dim.is_current"),
        col("dim.previous_name")
    )
scd1_df.show()

from pyspark.sql.functions import current_date, lit, col

#scd - 2
# Changed records
changed = dim_df.join(incoming_df, "customer_id") \
    .filter((dim_df.name != incoming_df.name) | (dim_df.email != incoming_df.email)) \
    .filter(col("is_current") == True)

# Expire old
expired = changed.withColumn("end_date", current_date()) \
                 .withColumn("is_current", lit(False))

# New current records
new_records = incoming_df.join(changed.select("customer_id"), "customer_id") \
    .select("customer_id", "name", "email") \
    .withColumn("start_date", current_date()) \
    .withColumn("end_date", lit(None).cast("date")) \
    .withColumn("is_current", lit(True)) \
    .withColumn("previous_name", lit(None))

# Unchanged records
unchanged = dim_df.join(changed.select("customer_id"), "customer_id", "leftanti")

# New inserts
new_inserts = incoming_df.join(dim_df, "customer_id", "leftanti") \
    .select("customer_id", "name", "email") \
    .withColumn("start_date", current_date()) \
    .withColumn("end_date", lit(None).cast("date")) \
    .withColumn("is_current", lit(True)) \
    .withColumn("previous_name", lit(None))

# Combine all
scd2_df = unchanged.unionByName(expired) \
                   .unionByName(new_records) \
                   .unionByName(new_inserts)

scd2_df.show()
