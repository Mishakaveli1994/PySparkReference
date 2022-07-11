"""
When we perform groupBy() on PySpark Dataframe, it returns GroupedData object
which contains below aggregate functions.

count() - Returns the count of rows for each group.

mean() - Returns the mean of values for each group.

max() - Returns the maximum of values for each group.

min() - Returns the minimum of values for each group.

sum() - Returns the total for values for each group.

avg() - Returns the average for values for each group.

agg() - Using agg() function, we can calculate more than one aggregate at a time.

pivot() - This function is used to Pivot the DataFrame
"""

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

schema = ["employee_name", "department", "state", "salary", "age", "bonus"]

spark = SparkSession.builder \
    .master("local[1]") \
    .appName("SparkByExamples.com") \
    .getOrCreate()

df = spark.createDataFrame(data=simpleData, schema=schema)
df.printSchema()
df.show(truncate=False)

'''GroupBy & Sum'''
print('GroupBy & Sum')
df.groupBy('department').sum('salary').show(truncate=False)

'''GroupBy & Count'''
print('GroupBy & Count')
df.groupBy('department').count().show()

'''GroupBy & Min'''
print('GroupBy & Min')
df.groupBy('department').min().show()

'''GroupBy & Max'''
print('GroupBy & Max')
df.groupBy('department').max().show()

'''GroupBy & Avg'''
print('GroupBy & Avg')
df.groupBy('department').avg().show()

'''GroupBy & Mean'''
print('GroupBy & Mean')
df.groupBy('department').mean().show()

'''GroupBy and Aggregate on multiple columns'''
print('GroupBy and Aggregate on multiple columns')
df.groupBy('department', 'state').sum('salary', 'bonus').show(truncate=False)

'''Multiple Aggregations'''
print('Multiple Aggregations')
df.groupBy('department').agg(sum('salary').alias('sum_salary'),
                             avg('salary').alias('avg_salary'),
                             sum('bonus').alias('sum_bonus'),
                             max('bonus').alias('max_bonus')).show(truncate=False)

'''Using filter on Aggregate Data'''
print('Using filter on Aggregate Data')
df.groupBy('department').agg(sum('salary').alias('sum_salary'),
                             avg('salary').alias('avg_salary'),
                             sum('bonus').alias('sum_bonus'),
                             max('bonus').alias('max_bonus')) \
    .where(col('sum_bonus') >= 50000).show(truncate=False)
