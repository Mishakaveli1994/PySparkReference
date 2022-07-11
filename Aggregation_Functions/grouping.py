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

'''grouping() Indicates whether a given input column is aggregated or not. 
Returns 1 for aggregated or 0 for not aggregated in the result. 
If you try grouping directly on the salary column you will get below error.'''

df.cube("department").agg(grouping("department"), sum("salary")).orderBy("department").show()
'''
Exception in thread "main" org.apache.spark.sql.AnalysisException:
  // grouping() can only be used with GroupingSets/Cube/Rollup
'''