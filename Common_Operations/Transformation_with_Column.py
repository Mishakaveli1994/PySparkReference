from pyspark.sql import SparkSession
from pyspark.sql.functions import *

data = [('James', '', 'Smith', '1991-04-01', 'M', 3000),
        ('Michael', 'Rose', '', '2000-05-19', 'M', 4000),
        ('Robert', '', 'Williams', '1978-09-05', 'M', 4000),
        ('Maria', 'Anne', 'Jones', '1967-12-01', 'F', 4000),
        ('Jen', 'Mary', 'Brown', '1980-02-17', 'F', -1)
        ]

columns = ["firstname", "middlename", "lastname", "dob", "gender", "salary"]

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
df = spark.createDataFrame(data=data, schema=columns)
df.printSchema()

'''Change column DataType'''
print('Change column DataType')
df.withColumn('salary', col('salary').cast('Integer')).show()

'''Update the value of an existing Column'''
print('Update the value of an existing Column')
df.withColumn('salary', col('salary') * 100).show()

'''Create a New Column for and existing one'''
print('Create a New Column for and existing one')
df.withColumn('CopiedColumn', col('salary') * -1).show()

'''Add a new column'''
print('Add a new column')
df.withColumn("Country", lit("USA")).withColumn("anotherColumn", lit("anotherValue")).show()

'''Rename Column Name'''
print('Rename Column Name')
df.withColumnRenamed('gender', 'sex').show(truncate=False)

'''Drop Column'''
print('Drop Column')
df.drop('salary').show()
