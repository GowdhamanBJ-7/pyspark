import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast
import file_path

spark = SparkSession.builder.appName("BroadcastJoinExample").getOrCreate()

large_df = spark.createDataFrame([(1, "Aa"), (2, "Bb"), (3, "Cc")], ["id", "name"])
small_df = spark.createDataFrame([(1, "HR"), (2, "Finance")], ["id", "department"])
# Start timer
start_time = time.time()
result_df = large_df.join(broadcast(small_df), on="id", how="inner")
result_df.show()
# End timer
end_time = time.time()
print(f"Join execution time: {end_time - start_time:.4f} seconds")
