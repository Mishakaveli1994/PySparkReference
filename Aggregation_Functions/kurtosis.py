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

'''kurtosis() function returns the kurtosis of the values in a group.
Kurtosis is a measure of whether the data are heavy-tailed or 
light-tailed relative to a normal distribution. That is, data 
sets with high kurtosis tend to have heavy tails, or outliers. 
Data sets with low kurtosis tend to have light tails, or lack of outliers. 
A uniform distribution would be the extreme case.
'''
df.select(kurtosis("salary")).show(truncate=False)