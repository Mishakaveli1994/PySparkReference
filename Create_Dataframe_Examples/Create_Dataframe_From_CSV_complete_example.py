from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType
from pyspark.sql.types import DoubleType, BooleanType

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

df = spark.read.csv("../Example_Sources/zipcodes.csv")

print('No headers')
df.printSchema()

df2 = spark.read.option("header", True) \
    .csv("../Example_Sources/zipcodes.csv")
print('First rows as headers')
df2.printSchema()

df3 = spark.read.options(header='True', delimiter=',') \
    .csv("../Example_Sources/zipcodes.csv")
print('First rows as headers')
df3.printSchema()

schema = StructType() \
    .add("RecordNumber", IntegerType(), True) \
    .add("Zipcode", IntegerType(), True) \
    .add("ZipCodeType", StringType(), True) \
    .add("City", StringType(), True) \
    .add("State", StringType(), True) \
    .add("LocationType", StringType(), True) \
    .add("Lat", DoubleType(), True) \
    .add("Long", DoubleType(), True) \
    .add("Xaxis", IntegerType(), True) \
    .add("Yaxis", DoubleType(), True) \
    .add("Zaxis", DoubleType(), True) \
    .add("WorldRegion", StringType(), True) \
    .add("Country", StringType(), True) \
    .add("LocationText", StringType(), True) \
    .add("Location", StringType(), True) \
    .add("Decommisioned", BooleanType(), True) \
    .add("TaxReturnsFiled", StringType(), True) \
    .add("EstimatedPopulation", IntegerType(), True) \
    .add("TotalWages", IntegerType(), True) \
    .add("Notes", StringType(), True)

df_with_schema = spark.read.format("csv").option("header", True).schema(schema).load("../Example_Sources/zipcodes.csv")
print('With Schema')
df_with_schema.printSchema()

df2.write.option("header", True) \
    .csv("../Example_Sources/zipcodes_new")
