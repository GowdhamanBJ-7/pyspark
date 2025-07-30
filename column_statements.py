from pyspark.sql.functions import col, when
from pyspark.sql import SparkSession
import file_path

spark = SparkSession.builder.appName("Numeric Function").getOrCreate()

df = spark.createDataFrame([(1, "AAA", 20), (2, "BBB", 40), (3, "CCC",50)], ["id", "name", "age"])
df2 = df.withColumn("id_plus_10", col("id") + 10)
df2 = df2.withColumn("id", col("id")+100)
df2.show()

print("column rename")
df2 = df2.withColumnRenamed("id_plus_10","new_employee_id")
df2.show()

print("drop column")
df2 = df2.drop("new_employee_id")
df2.show()

print("Alias name")
df.select(col('name').alias("employee_name")).show()

print("")
df3 = df.withColumn("level",
                    when(col("age")> 35, "senior")
                    .when(col("age")<=34, "junior")
                    .when(col("age").isNull(),"Null Record")
                    .otherwise("Unknown"))
df3.show()