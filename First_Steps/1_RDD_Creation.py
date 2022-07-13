# Import SparkSession_SparkContext
from pyspark.sql import SparkSession

# Create SparkSession_SparkContext
spark = SparkSession.builder \
    .master("local[1]") \
    .appName("SparkByExamples.com") \
    .getOrCreate()

# Create RDD from parallelize
dataList = [("Java", 20000), ("Python", 100000), ("Scala", 3000)]
rdd = spark.sparkContext.parallelize(dataList)

# Create RDD from external Data source
rdd2 = spark.sparkContext.textFile("/path/test.txt")
