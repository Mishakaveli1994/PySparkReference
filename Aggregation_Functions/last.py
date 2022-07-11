from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName('pyspark-by-examples').getOrCreate()

simpleData = [("James", "Sales", 3000),
              ("Michael", "Sales", 4600),
              ("Robert", "Sales", 4100),
              ("Maria", "Finance", 3000),
              ("James", "Sales", 3000),
              ("Scott", "Finance", 3300),
              ("Jen", "Finance", 3900),
              ("Jeff", "Marketing", 3000),
              ("Kumar", "Marketing", 2000),
              ("Saif", "Sales", 4100)
              ]
schema = ["employee_name", "department", "salary"]
df = spark.createDataFrame(data=simpleData, schema=schema)
df.printSchema()
df.show(truncate=False)

'''last() function returns the last element in a column. 
When ignoreNulls is set to true, it returns the last non-null element.'''
df.select(last("salary")).show(truncate=False)
