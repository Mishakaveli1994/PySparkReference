from pyspark.sql import Row
from pyspark.sql import SparkSession

columns = ["language", "users_count"]
data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

# Create from SparkSession_SparkContext
dfFromData2 = spark.createDataFrame(data).toDF(*columns)

# Create with createDataFrame with Row type
rowData = list(map(lambda x: Row(*x), data))
dfFromData3 = spark.createDataFrame(rowData, columns)

print(rowData)
dfFromData3.printSchema()
