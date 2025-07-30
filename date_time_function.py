import file_path
from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, to_date, to_timestamp, date_format,
                                   current_date, current_timestamp, year, month,
                                   weekofyear, dayofyear, dayofweek, day, datediff, add_months, date_add, date_sub)

spark = SparkSession.builder.appName("date and time function").getOrCreate()

data = [
    (1, "2024-09-23", "2024-11-20"),
    (2, "2023-10-05", "2023-10-15"),
    (3, "2022-05-01", "2022-05-10")
]

df = spark.createDataFrame(data, ["id", "order_date_string", "delivery_date"])
# df.printSchema()
#
# to_date
df = df.withColumn("order_date", to_date(col("order_date_string"), "yyyy-MM-dd"))
df = df.withColumn("delivery_date", to_date(col("delivery_date"), "yyyy-MM-dd"))
df.printSchema()
print("to_date: string to date convertion")
df.show()
#
# # to_timestamp
# data = [("2024-07-30 14:25:00",), ("2024-07-30 09:05:00",)]
# df1 = spark.createDataFrame(data, ["datetime_str"])
#
# df1 = df1.withColumn("timestamp_col", to_timestamp("datetime_str", "yyyy-MM-dd HH:mm:ss"))
# print("time stamp")
# df1.show()
#
# #date formatting
# print("date formating")
# df.select( date_format(df["order_date"], "d/M/yy").alias("formatted date")).show()
# df = df.withColumn("format_date",date_format(df["order_date"], 'dd-MM-yy'))
# df.show()

# # current_date()
#
# df = df.withColumn("current date", current_date())
# df = df.withColumn("current timestamp", current_timestamp())
#
# df.show(truncate = False)
# df.printSchema()

# Extracting parts of dates

#year
df.select(
    year("order_date").alias("extract_year"),
    weekofyear("order_date").alias("week_number"),
    dayofyear("order_date").alias("day_number")
).show()

#month
df.select(
    month("order_date").alias("extract_month"),
    dayofweek("order_date").alias("week_of_day")
).show()

df.select(
    day("order_date").alias("day")
).show()

df.select(
    datediff(df["delivery_date"], df["order_date"]).alias("days_diff")
).show()
