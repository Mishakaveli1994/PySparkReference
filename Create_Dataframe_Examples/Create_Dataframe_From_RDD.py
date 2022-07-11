from pyspark.sql import SparkSession

columns = ["language", "users_count"]
data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
rdd = spark.sparkContext.parallelize(data)

# With toDF
# When no column names are provided, the columns respectively are _1 and _2
dfFromRDD1 = rdd.toDF(columns)
dfFromRDD1.printSchema()
dfFromRDD1.show()

# With createDataFrame
dfFromRDD2 = spark.createDataFrame(rdd).toDF(*columns)