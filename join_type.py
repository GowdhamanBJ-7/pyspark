import file_path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum

spark = SparkSession.builder.appName("joins").getOrCreate()

emp = [(1,"Red",1,"2018","10","M",3000),
       (2,"Green",2,"2010","20","M",4000),
       (3,"Yellow",1,"2010","10","M",1000),
       (4,"White",2,"2005","10","F",2000),
       (5,"Black",2,"2010","40","",None),
       (6,"Brown",2,"2010","50","",None)
]
empColumns = ["emp_id","name","superior_emp_id","year_joined","emp_dept_id","gender","salary"]

dept = [("Finance",10),
    ("Marketing",20),
    ("Sales",30),
    ("IT",40)
  ]

deptColumns = ["department","dept_id"]
df1 = spark.createDataFrame(emp, empColumns)
df2 = spark.createDataFrame(dept, deptColumns)

print("employee dataframe")
df1.show()

print("department dataframe")
df2.show()

#inner join
print("Inner join")
df_join = df1.join(df2, df1.emp_dept_id == df2.dept_id, 'inner')
df_join.show()

df_join= df_join.fillna({"salary":0})
df_join = df_join.groupBy("department").agg(sum("salary").alias("total_salary"))
df_join.show()

#left, left_outer, leftouter
print("left join")
df1.join(df2, df1.emp_dept_id == df2.dept_id, 'left').show()

#right, rightouter, right_outer
print("right join")
df1.join(df2, df1.emp_dept_id == df2.dept_id, 'right').show()

#left semi --> semi, leftsemi, left_semi
print("left semi join")
df1.join(df2, df1.emp_dept_id == df2.dept_id, "semi").show()

# left anti --> anti, leftanti, left_anti
print("left anti join")
df1.join(df2, df1.emp_dept_id == df2.dept_id, "anti").show()