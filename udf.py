from pyspark.sql import SparkSession
import file_path
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, IntegerType

spark = SparkSession.builder.appName("udf").getOrCreate()
df = spark.createDataFrame([("arun",38),("ravi",62)], ["name","age"])

#python function
def reverse_string(value):
    return value[::-1]

#convert to UDF
reverse_udf = udf(reverse_string, StringType())

df_reverse = df.withColumn("reversed_name", reverse_udf(df["name"]))
df_reverse.show()

#
def age_category(age):
    if age<18:
        return 'child'
    elif 18 <= age < 50:
        return 'young'
    else:
        return 'senior'


age_udf = udf(age_category, StringType())
df_age = df.withColumn("category", age_udf(df["age"]))
df_age.show()

#---use decorator
@udf(returnType=StringType())
def age_category_udf(age):
    if age<18:
        return 'child'
    elif 18 <= age < 50:
        return 'young'
    else:
        return 'senior'


df_age_de = df.withColumn("category_de", age_category_udf(df["age"]))
df_age_de.show()