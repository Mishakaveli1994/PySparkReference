from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").appName("SparkByExamples.com").getOrCreate()
# Create DataFrame from JSON
df = spark.read.json("../Example_Sources/zipcodes.json")
df.printSchema()
df.show()



