'''
Drop Function

Syntax: drop(how='any', thresh=None, subset=None)

how – This takes values ‘any’ or ‘all’. By using ‘any’, drop a row if it contains NULLs on any columns.
      By using ‘all’, drop a row only if all columns have NULL values. Default is ‘any’.
thresh – This takes int value, Drop rows that have less than thresh hold non-null values. Default is ‘None’.
subset – Use this to select the columns for NULL values. Default is `None`.
'''

from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").appName("SparkByExamples.com").getOrCreate()

filePath = '../Example_Sources/small_zipcode.csv'
df = spark.read.options(header='true', inferSchema='true').csv(filePath)

df.printSchema()
df.show(truncate=False)

'''Remove rows that have a column with value NULL'''
print('Remove rows that have a column with value NULL')
df.na.drop().show(truncate=False)
df.na.drop('any').show(truncate=False)

'''Remove rows that have NULL value in all columns'''
print('Remove rows that have NULL value in all columns')
df.na.drop('all').show(truncate=False)

'''Remove Rows with NULL Value of Selected Columns'''
print('Remove Rows with NULL Value of Selected Columns')
df.na.drop(subset=['population', 'type']).show(truncate=False)

'''Remove Rows with NULL Values with dropna'''
print('Remove Rows with NULL Values with dropna')
# drop(columns:Seq[String]) or drop(columns:Array[String])
df.dropna().show(truncate=False)
