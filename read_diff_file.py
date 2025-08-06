from Tools.scripts.generate_opcode_h import header

import file_path
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, ArrayType

spark = SparkSession.builder.appName("file_format").getOrCreate()

#method -1
# df = spark.read.csv(path="C:\\Users\\GowdhamanBJ\\Downloads\\customers-100.csv", header=True, inferSchema=True)
# df.printSchema()
# df.show(5)
#
# #method - 2
# df1 = (spark.read.format("csv")
#        .option(key='header', value=True)
#        .option(key='inferSchema', value=True)
#        .load(path="C:\\Users\\GowdhamanBJ\\Downloads\\customers-100.csv"))
# df1.show(4)
#
# #Multiple CSV file
# # "C:\Users\GowdhamanBJ\Downloads\products-100.csv"
# #"C:\Users\GowdhamanBJ\Downloads\Department-Q1.csv"
#
# df_multi = spark.read.csv(path=["C:\\Users\\GowdhamanBJ\\Downloads\\Employee-Q1.csv",
#                                 "C:\\Users\\GowdhamanBJ\\Downloads\\Department-Q1.csv"],
#                           header=True, inferSchema=True)
# df_multi.show()
#
# # #read csv file with the schema
# # # Define schema
# # csv_schema = StructType([
# #     StructField("name", StringType(), True),
# #     StructField("age", IntegerType(), True)
# # ])
# #
# # # Read CSV file with schema
# # df_csv = spark.read.csv("path/to/file.csv", header=True, schema=csv_schema)
#
#
# #read Parquet file
# # df_parquet = spark.read.parquet("C:\\Users\\GowdhamanBJ\\Downloads\\mtcars.parquet")
# df_parquet = spark.read.format("parquet").load("C:\\Users\\GowdhamanBJ\\Downloads\\mtcars.parquet")
# print("Parquet file")
# df_parquet.printSchema()
# df_parquet.show(5)


#read JSON file
#"C:\\Users\\GowdhamanBJ\\Downloads\\sample1.json"

df_json = (spark.read.format("json")
           .option(key="multiline", value="True")
           .load("C:\\Users\\GowdhamanBJ\\Downloads\\sample1.json"))
print('json file')
df_json.show()

schema_json = StructType([
    StructField("name", StringType(), False),
    StructField("age", IntegerType(), False),
    StructField("city", StringType(),False),
    StructField("married",BooleanType(), False),
    StructField("hobbies",ArrayType(StringType()),True),
    StructField("address", StructType([
        StructField("street", StringType(), True),
        StructField("city", StringType(), True),
        StructField("postal_code", StringType(), True),
    ]), True)
])

df_schema_json = (spark.read.schema(schema_json)
                  .option(key="multiline", value="True")
                  .json("C:\\Users\\GowdhamanBJ\\Downloads\\sample1.json"))
print("schema")
df_schema_json.printSchema()
df_schema_json.show(truncate=False)