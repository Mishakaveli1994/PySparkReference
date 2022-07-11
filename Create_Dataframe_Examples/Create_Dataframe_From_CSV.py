from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").appName("SparkByExamples.com").getOrCreate()
# Create DataFrame from CSV with no headers
df = spark.read.csv("../Example_Sources/zipcodes.csv")
df.printSchema()
df.show()

'''
Other possible syntax
df = spark.read.format("csv").load("../Example_Sources/zipcodes.csv")
df = spark.read.format("org.apache.spark.sql.csv").load("../Example_Sources/zipcodes.csv")

org.apache.spark.sql.csv is the fully qualified name for csv
'''

# Create DataFrame from CSV with 1st row as header
df2 = spark.read.option("header", True).csv("../Example_Sources/zipcodes.csv")
df2.printSchema()
df2.show()

'''
Read Multiple CSV Files
df = spark.read.csv("path1,path2,path3")

Read all CSV files in a Directory
df = spark.read.csv("Folder path")
'''

'''
Provide Options
Chaining option(self, key, value) to use multiple options
df4 = spark.read.option("inferSchema",True).option("delimiter",",").csv("../Example_Sources/zipcodes.csv")

Or use multiple options at once options(self, **options) method
df4 = spark.read.options(inferSchema='True',delimiter=',').csv("../Example_Sources/zipcodes.csv")

'''

'''
Options

delimiter - used to specify the column delimiter of the CSV file. By default, it is comma (,) character,
but can be set to any character like pipe(|), tab (\t), space using this option.

inferSchema - The default value set to this option is False when setting to true it automatically infers column 
types based on the data. Note that, it requires reading the data one more time to infer the schema.

header - This option is used to read the first line of the CSV file as column names. By default the value of this 
option is False , and all column types are assumed to be a string.

quotes - When you have a column with a delimiter that used to split the columns, use quotes option to specify 
the quote character, by default it is ” and delimiters inside quotes are ignored. but using this option you 
can set any character

nullValues - Using nullValues option you can specify the string in a CSV to consider as null. 
For example, if you want to consider a date column with a value "1900-01-01" set null on DataFrame.

dateFormat - dateFormat option to used to set the format of the input DateType and TimestampType
columns. Supports all java.text.SimpleDateFormat formats.
 
Full Documentation - https://docs.databricks.com/data/data-sources/read-csv.html
'''

'''
Reading CSV files with a user-specified custom schema
schema = StructType() \
      .add("RecordNumber",IntegerType(),True) \
      .add("Zipcode",IntegerType(),True) \
      .add("ZipCodeType",StringType(),True) \
      .add("City",StringType(),True) \
      .add("State",StringType(),True) \
      .add("LocationType",StringType(),True) \
      .add("Lat",DoubleType(),True) \
      .add("Long",DoubleType(),True) \
      .add("Xaxis",IntegerType(),True) \
      .add("Yaxis",DoubleType(),True) \
      .add("Zaxis",DoubleType(),True) \
      .add("WorldRegion",StringType(),True) \
      .add("Country",StringType(),True) \
      .add("LocationText",StringType(),True) \
      .add("Location",StringType(),True) \
      .add("Decommisioned",BooleanType(),True) \
      .add("TaxReturnsFiled",StringType(),True) \
      .add("EstimatedPopulation",IntegerType(),True) \
      .add("TotalWages",IntegerType(),True) \
      .add("Notes",StringType(),True)
      
df_with_schema = spark.read.format("csv") \
      .option("header", True) \
      .schema(schema) \
      .load("/tmp/resources/zipcodes.csv")
'''

'''
Write PySpark DataFrame to CSV file
df.write.option("header", True).csv('../Example_Sources/zipcodes.csv')

Saving Modes
overwrite – mode is used to overwrite the existing file.

append – To add the data to the existing file.

ignore – Ignores write operation when the file already exists.

error – This is a default option when the file already exists, it returns an error.
-----------------------------------------------------------------------------------------
df2.write.mode('overwrite').csv("../Example_Sources/zipcodes.csv")
OR
df2.write.format("csv").mode('overwrite').save("../Example_Sources/zipcodes.csv")
'''
