from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import SparkSession

schema = StructType([
    StructField('name', StructType([
        StructField('firstname', StringType(), True),
        StructField('middlename', StringType(), True),
        StructField('lastname', StringType(), True)
    ])),
    StructField('dob', StringType(), True),
    StructField('gender', StringType(), True),
    StructField('salary', IntegerType(), True)
])

dataDF = [(('James', '', 'Smith'), '1991-04-01', 'M', 3000),
          (('Michael', 'Rose', ''), '2000-05-19', 'M', 4000),
          (('Robert', '', 'Williams'), '1978-09-05', 'M', 4000),
          (('Maria', 'Anne', 'Jones'), '1967-12-01', 'F', 4000),
          (('Jen', 'Mary', 'Brown'), '1980-02-17', 'F', -1)
          ]

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
df = spark.createDataFrame(data=dataDF, schema=schema)
df.printSchema()

'''Rename single non-nested column'''
print('Rename single non-nested column')
df.withColumnRenamed("dob", "DateOfBirth").printSchema()

'''Rename multiple columns with chaining the command'''
print('Rename multiple columns with chaining the command')
df2 = df.withColumnRenamed("dob", "DateOfBirth").withColumnRenamed("salary", "salary_amount")
df2.printSchema()

'''Rename nested column'''
# 1. Create new schema
print('Create new schema and update in existing DataFrame')
schema2 = StructType([
    StructField("fname", StringType()),
    StructField("middlename", StringType()),
    StructField("lname", StringType())])
# Update schema in existing DataFrame
df.select(col("name").cast(schema2), col("dob"), col("gender"), col("salary")).printSchema()

'''Using select to rename nested columns'''
print('Using select to rename nested columns')
df.select(col('name.firstname').alias('fname'),
          col('name.middlename').alias('mname'),
          col('name.lastname').alias('lname'),
          col('dob'), col('gender'), col('salary')).printSchema()

'''Using DataFrame withColumn to rename nested columns. Will remove nested structure'''
print('Using DataFrame withColumn to rename nested columns. Will remove nested structure')
df4 = df.withColumn('fname', col('name.firstname')) \
    .withColumn('mname', col('name.middlename')) \
    .withColumn('lname', col('name.lastname')) \
    .drop('name')
df4.printSchema()

'''Using toDF() to change all columns in a PySpark DataFrame'''
print('Using toDF() to change all columns in a PySpark DataFrame')
newColumns = ["newCol1", "newCol2", "newCol3", "newCol4"]
df.toDF(*newColumns).printSchema()
