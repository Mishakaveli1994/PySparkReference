from pyspark.sql import SparkSession
from pyspark.sql.functions import *

simpleData = [("James", "Sales", "NY", 90000, 34, 10000),
              ("Michael", "Sales", "NY", 86000, 56, 20000),
              ("Robert", "Sales", "CA", 81000, 30, 23000),
              ("Maria", "Finance", "CA", 90000, 24, 23000),
              ("Raman", "Finance", "CA", 99000, 40, 24000),
              ("Scott", "Finance", "NY", 83000, 36, 19000),
              ("Jen", "Finance", "NY", 79000, 53, 15000),
              ("Jeff", "Marketing", "CA", 80000, 25, 18000),
              ("Kumar", "Marketing", "NY", 91000, 50, 21000)
              ]
columns = ["employee_name", "department", "state", "salary", "age", "bonus"]

spark = SparkSession.builder.master("local[1]").appName("SparkByExamples.com").getOrCreate()
df = spark.createDataFrame(data=simpleData, schema=columns)
df.printSchema()
df.show(truncate=False)

'''Sort Function - by default ascending'''
print('Default Sort')
df.sort('department', 'state').show(truncate=False)
df.sort(col('department'), col('state')).show(truncate=False)

'''OrderBy Function - by default ascending'''
print('Default OrderBy')
df.orderBy('department', 'state').show(truncate=False)
df.orderBy(col('department'), col('state')).show(truncate=False)

'''Sort by Ascending'''
print('Ascending Sorting')
df.sort(df.department.asc(), df.state.asc()).show(truncate=False)
df.sort(col('department').asc(), col('state').asc()).show(truncate=False)
df.orderBy(col('department').asc(), col('state').asc()).show(truncate=False)

'''Sort by Descending'''
print('Descending Sorting')
df.sort(df.department.asc(), df.state.desc()).show(truncate=False)
df.sort(col('department').asc(), col('state').desc()).show(truncate=False)
df.orderBy(col('department').asc(), col('state').desc()).show(truncate=False)

'''Raw SQL Sorting'''
print('Raw SQL Sorting')
df.createOrReplaceTempView('EMP')
spark.sql('SELECT employee_name, department, state, salary, age, bonus FROM EMP ORDER BY department ASC').show(
    truncate=False)
