from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").appName("SparkByExamples.com").getOrCreate()
# Create DataFrame from TXT
df = spark.read.text("../Example_Sources/test.txt")
df.printSchema()
df.show()

