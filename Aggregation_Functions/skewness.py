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

'''skewness() function returns the skewness of the values in a group.
A skewness value greater than 1 or less than -1 indicates a highly 
skewed distribution. A value between 0.5 and 1 or -0.5 and -1 is 
moderately skewed. A value between -0.5 and 0.5 indicates that the 
distribution is fairly symmetrical.'''

df.select(skewness("salary")).show(truncate=False)