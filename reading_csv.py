import file_path

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("read new csv file").getOrCreate()

df_csv_file = spark.read.csv("C:\\Users\\GowdhamanBJ\\Downloads\\customers-100.csv", header = True)
df_csv_file.printSchema()
df_csv_file.show()

