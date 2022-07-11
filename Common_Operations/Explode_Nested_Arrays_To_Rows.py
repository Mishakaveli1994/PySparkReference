from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName('pyspark-by-examples').getOrCreate()

arrayArrayData = [
    ("James", [["Java", "Scala", "C++"], ["Spark", "Java"]]),
    ("Michael", [["Spark", "Java", "C++"], ["Spark", "Java"]]),
    ("Robert", [["CSharp", "VB"], ["Spark", "Python"]])
]

df = spark.createDataFrame(data=arrayArrayData, schema=['name', 'subjects'])
df.printSchema()
df.show(truncate=False)

'''Explode Nested Arrays'''
print('Explode Nested Arrays')
df.select(df.name, explode(df.subjects)).show(truncate=False)

'''Flatten Nested Arrays'''
print('Flatten Nested Arrays')
df.select(df.name, flatten(df.subjects)).show(truncate=False)
