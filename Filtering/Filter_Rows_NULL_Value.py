from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .master("local[1]") \
    .appName("SparkByExamples.com") \
    .getOrCreate()

data = [
    ("James", None, "M"),
    ("Anna", "NY", "F"),
    ("Julia", None, None)
]

columns = ["name", "state", "gender"]
df = spark.createDataFrame(data, columns)
df.show()

'''Filter Rows with NULL Values in DataFrame'''
print('Filter Rows with NULL Values in DataFrame')
df.filter('state IS NULL').show()
df.filter(df.state.isNull()).show()
df.filter(col('state').isNull()).show()

'''Filter Rows with NULL on multiple columns'''
print('Filter Rows with NULL on multiple columns')
df.filter('state IS NULL AND gender IS NULL').show()
df.filter(df.state.isNull() & df.gender.isNull()).show()

'''Filter Rows with IS NOT NULL or isNotNull'''
print('Filter Rows with IS NOT NULL or isNotNull')
df.filter('state IS NOT NULL').show()
df.filter('NOT state IS NULL').show()
df.filter(df.state.isNotNull()).show()
df.filter(col('state').isNotNull()).show()
df.na.drop(subset=["state"]).show()

'''SQL Filter Rows with NULL Values'''
print('SQL Filter Rows with NULL Values')
df.createOrReplaceTempView("DATA")
spark.sql("SELECT * FROM DATA where STATE IS NULL").show()
spark.sql("SELECT * FROM DATA where STATE IS NULL AND GENDER IS NULL").show()
spark.sql("SELECT * FROM DATA where STATE IS NOT NULL").show()
