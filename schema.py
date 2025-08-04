import file_path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import (StructType, StructField,
                               IntegerType, FloatType, StringType, ArrayType, MapType)

spark = SparkSession.builder.appName("schema_definition").getOrCreate()

data1 = [
    ("James", "", "Smith", "36636", "M", 3000),
    ("Michael", "Rose", "", "40288", "M", 4000),
    ("Robert", "", "Williams", "42114", "M", 4000),
    ("Maria", "Anne", "Jones", "39192", "F", 4000),
    ("Jen", "Mary", "Brown", "", "F", -1)
]
schema1 = StructType([
    StructField("firstname", StringType(), False),
    StructField("middlename", StringType(), True),
    StructField("lastname", StringType(), True),
    StructField("id", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("salary", IntegerType(), True)
])

basic_df = spark.createDataFrame(data=data1, schema=schema1)
basic_df.printSchema()
# basic_df.show()

#Nested StructType (Struct inside Struct)
data2 = [
    (("James", "", "Smith"), "36636", "M", 3100),
    (("Michael", "Rose", ""), "40288", "M", 4300),
    (("Robert", "", "Williams"), "42114", "M", 1400),
    (("Maria", "Anne", "Jones"), "39192", "F", 5500),
    (("Jen", "Mary", "Brown"), "", "F", -1)
]

Schema2 = StructType([
    StructField("name", StructType([
        StructField("firstname", StringType(), False),
        StructField("middlename", StringType(), True),
        StructField("lastname", StringType(), True)
    ])),
    StructField("id", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("salary", IntegerType(), True)
])

nested_df = spark.createDataFrame(data=data2, schema=Schema2)
nested_df.printSchema()
# nested_df.show()

#
arrayStructureData = [
    (("James", "", "Smith"), ["music", "reading", "chess"], {"salary": "3000", "dept": "HR"}),
    (("Michael", "Rose", ""), ["music", "playing", "chess"], {"salary": "3000", "dept": "HR"}),
    (("Robert", "", "Williams"), ["music", "reading", "chess"], {"salary": "3000", "dept": "HR"}),
    (("Maria", "Anne", "Jones"), ["music", "reading", "chess"], {"salary": "3000", "dept": "HR"}),
    (("Jen", "Mary", "Brown"), ["music", "reading", "chess"], {"salary": "3000", "dept": "HR"})
]

Schema3 = StructType([
    StructField("name", StructType([
        StructField("firstname", StringType(), True),
        StructField("middlename", StringType(), True),
        StructField("lastname", StringType(), True)
    ])),
    StructField("hobbies", ArrayType(StringType()), True),
    StructField("properties", MapType(StringType(), StringType()), True)
])

array_df = spark.createDataFrame(data=arrayStructureData, schema=Schema3)
array_df.printSchema()
# array_df.show(truncate=False)


#schema function
basic_df.printSchema()

print(basic_df.schema)

print("df.schema.json()")
print(basic_df.schema.json())

#comparing schema
if basic_df.schema == nested_df.schema:
    print("schema match: do some operation")
else:
    print("perform any other operations")

print("To list the columns that where missing in nested_df")
print(list(set(basic_df.schema) - set(nested_df.schema)))


allColumns = basic_df.columns + nested_df.columns
uniqueColumns = list(set(allColumns))
print(uniqueColumns)

from pyspark.sql.functions import lit

for col in uniqueColumns:
    if col not in basic_df.columns:
        basic_df = basic_df.withColumn(col, lit(None))
    if col not in nested_df.columns:
        nested_df = nested_df.withColumn(col, lit(None))

basic_df.show()
nested_df.show()