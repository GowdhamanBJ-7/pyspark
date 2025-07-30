import file_path
from pyspark.sql import SparkSession

spark  = SparkSession.builder.appName("general dataframe funciton").getOrCreate()
data = [
    (1, "Arun", "Chennai", "India", 30),
    (2, "Karthi", "Hosur", "India", 23),
    (3, "Kavin", "Salem", "India", 31),
    (4, "Ravi", "Bangalore", "India", 25),
    (5, "Kumar", "Bangalore", "India", 25),
    (6, "Vijay", "Chennai", "India", 30)
]

df = spark.createDataFrame(data,["id","name","city","country","age"])

# show
df.show()
df.show(3)

#collect
row = df.collect()
for r in row:
    print(r)

print(row)

# take(n)
some_row = df.take(2)
print(some_row)
for s in some_row:
    print(s)

# schema
df.printSchema()

# count
df.count()

# select
df.select("name","age").show()
df.select(df["name"].alias("user name")).show()
df.select(df.id).show()

df.filter(df["age"] == 30).show()
df.filter((df["age"] == 30) & (df["name"] == "Arun")).show()

df.where(df["city"] == "Chennai").show()

df.filter(df["name"].like("A%")).show()

# df.sort().show() # not possible
df.sort("age").show()
df.sort(df["age"].desc()).show()

df.describe().show()


print(df.columns)
# df.columns.show()

