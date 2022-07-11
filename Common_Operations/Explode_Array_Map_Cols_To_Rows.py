from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName('pyspark-by-examples').getOrCreate()

arrayData = [
    ('James', ['Java', 'Scala'], {'hair': 'black', 'eye': 'brown'}),
    ('Michael', ['Spark', 'Java', None], {'hair': 'brown', 'eye': None}),
    ('Robert', ['CSharp', ''], {'hair': 'red', 'eye': ''}),
    ('Washington', None, None),
    ('Jefferson', ['1', '2'], {})]

df = spark.createDataFrame(data=arrayData, schema=['name', 'knownLanguages', 'properties'])
df.printSchema()
df.show()

''' 
Explode - used to explode or create array or map columns to rows
When an array is passed to this function, it creates a new default column “col1” and it contains 
all array elements. 
When a map is passed, it creates two new columns one for key and one for value and each 
element in map split into the rows.
Will ignore entries that have NULL values
'''
# Explode Array
print('Explode Array')
df2 = df.select(df.name, explode(df.knownLanguages))
df2.printSchema()
df2.show()

# Explode Map (Dictionary)
print('Explode Map (Dictionary)')
df3 = df.select(df.name, explode(df.properties))
df3.printSchema()
df3.show()

'''
Explore Outer - used to create a row for each element in the 
array or map column. Unlike explode, if the array or map is null or
empty, explode_outer returns null.
'''

# Explode Outer Array
print('Explode Outer Array')
df.select(df.name, explode_outer(df.knownLanguages)).show()

# Explode Outer Map (Dictionary)
print('Explode Outer Map')
df.select(df.name, explode_outer(df.properties)).show()

'''
Posexplode - creates a row for each element in the array and creates two columns 
“pos’ to hold the position of the array element and the ‘col’ to hold the actual 
array value. And when the input column is a map, posexplode function creates 3 columns
 “pos” to hold the position of the map element, “key” and “value” columns.
This will ignore elements that have null or empty.
'''

# Posexplode Array
print('Posexplode Array')
df.select(df.name, posexplode(df.knownLanguages)).show()

# Posexplode Map (Dictionary)
print('Posexplode Map')
df.select(df.name, posexplode(df.properties)).show()

'''
Posexplode Outer - creates a row for each element in the array and
creates two columns “pos’ to hold the position of the 
array element and the ‘col’ to hold the actual array value. 
Unlike posexplode, if the array or map is null or empty, posexplode_outer 
function returns null, null for pos and col columns. Similarly for 
the map, it returns rows with nulls.
'''

# Posexplode Outer Array
print('Posexplode Outer Array')
df.select(df.name, posexplode_outer(df.knownLanguages)).show()

# Posexplode Outer Map (Dictionary)
print('Posexplode Outer Map')
df.select(df.name, posexplode_outer(df.properties)).show()