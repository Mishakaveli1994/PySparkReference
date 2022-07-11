from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, ArrayType, StructType, StructField
from pyspark.sql.functions import *

data = [
    (("James", "", "Smith"), ["Java", "Scala", "C++"], "OH", "M"),
    (("Anna", "Rose", ""), ["Spark", "Java", "C++"], "NY", "F"),
    (("Julia", "", "Williams"), ["CSharp", "VB"], "OH", "F"),
    (("Maria", "Anne", "Jones"), ["CSharp", "VB"], "NY", "M"),
    (("Jen", "Mary", "Brown"), ["CSharp", "VB"], "NY", "M"),
    (("Mike", "Mary", "Williams"), ["Python", "VB"], "OH", "M")
]

schema = StructType([
    StructField('name', StructType([
        StructField('firstname', StringType(), True),
        StructField('middlename', StringType(), True),
        StructField('lastname', StringType(), True)
    ])),
    StructField('languages', ArrayType(StringType()), True),
    StructField('state', StringType(), True),
    StructField('gender', StringType(), True)
])

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
df = spark.createDataFrame(data=data, schema=schema)
df.printSchema()
df.show(truncate=False)

'''Filter with Column Condition'''
# filter == where -> can be used interchangeably
# Equals condition
print('Equals condition')
df.filter(df.state == 'OH').show(truncate=False)

# Not equal condition
print('Not equal condition')
df.filter(df.state != 'OH').show(truncate=False)
df.filter(~(df.state == 'OH')).show(truncate=False)

# Using col
print('Using col')
df.filter(col("state") == "OH").show(truncate=False)

'''Filter with SQL Expression'''
# Equals condition
print('SQL Equals condition')
df.filter("gender == 'M'").show()

# Not equal condition
print('SQL Not equal condition')
df.filter("gender != 'M'").show()
df.filter("gender <> 'M'").show()

'''Multiple Conditions'''
print('Multiple Conditions')
# & - and
# | - or
# ! - not
df.filter((df.state == 'OH') & (df.gender == 'M')).show(truncate=False)

'''Filter based on List values'''
print('Filter based on List values')
li = ['OH', 'CA', 'DE']

# Equals Condition
print('Equals Condition')
df.filter(df.state.isin(li)).show()

# Not equal condition
print('Not equal condition')
df.filter(~df.state.isin(li)).show()
df.filter(df.state.isin(li) == False).show()

'''Starts With, Ends With, Contains'''
# Using Starts With
print('Using Starts With')
df.filter(df.state.startswith('N')).show()
# Using Ends With
print('Using Ends With')
df.filter(df.state.endswith('H')).show()
# Using Contains
print('Using Contains')
df.filter(df.state.contains('H')).show()

'''Like and Rlike'''
print('Like and Rlike')
data2 = [(2, "Michael Rose"), (3, "Robert Williams"),
         (4, "Rames Rose"), (5, "Rames rose")
         ]
df2 = spark.createDataFrame(data=data2, schema=['id', 'name'])

# Like - SQL LIKE Pattern
print('Like - SQL LIKE Pattern')
df2.filter(df2.name.like("%rose%")).show()

# Rlike - SQL RLIKE Pattern (Regex Like)
print('Rlike - SQL RLIKE Pattern (Regex Like)')
df2.filter(df2.name.rlike("(?i)^*rose$")).show()

'''Filter on an Array Column - returns entries if value in array field'''
print('Filter on an Array Column - returns entries if value in array field')
df.filter(array_contains(df.languages, 'Java')).show(truncate=False)

'''Filtering nested Struct Columns'''
print('Filtering nested Struct Columns')
df.filter(df.name.lastname == 'Williams').show(truncate=False)
