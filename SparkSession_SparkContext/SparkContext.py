'''
pyspark.SparkContext is an entry point to the PySpark functionality that is used to communicate with the
cluster and to create an RDD, accumulator, and broadcast variables. In this article, you will learn how to
create PySpark SparkContext with examples. Note that you can create only one SparkContext per JVM, in order to
create another first you need to stop the existing one using stop() method.
'''

# Get app name
'''
sc.appName
'''


# Create SparkSession from builder and get app name
'''from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[1]") \
                    .appName('SparkByExamples.com') \
                    .getOrCreate()
print(spark.sparkContext)
print("Spark App Name : "+ spark.sparkContext.appName)

# Outputs
# <SparkContext master=local[1] appName=SparkByExamples.com>
# Spark App Name : SparkByExamples.com'''

# Stop SparkContext
'''
spark.sparkContext.stop()
'''

# Creating SparkContext prior to PySpark 2.0

'''
You can create SparkContext by programmatically using its constructor, and pass parameters like master and appName at least as these are mandatory params. The below example creates context with a master as local and app name as Spark_Example_App.


# Create SparkContext
from pyspark import SparkContext
sc = SparkContext("local", "Spark_Example_App")
print(sc.appName)
Python
You can also create it using SparkContext.getOrCreate(). It actually returns an existing active SparkContext otherwise creates one with a specified master and app name.


# Create Spark Context
from pyspark import SparkConf, SparkContext
conf = SparkConf()
conf.setMaster("local").setAppName("Spark Example App")
sc = SparkContext.getOrCreate(conf)
print(sc.appName)
'''

# SparkContext commonly used variables

'''
applicationId – Returns a unique ID of a PySpark application.

version – Version of PySpark cluster where your job is running.

uiWebUrl – Provides the Spark Web UI url that started by SparkContext.
'''

# SparkContext commonly used methods

'''
accumulator(value[, accum_param]) – It creates an pyspark accumulator variable with initial specified value. 
Only a driver can access accumulator variables.

broadcast(value) – read-only PySpark broadcast variable. This will be broadcast to the entire cluster. 
You can broadcast a variable to a PySpark cluster only once.

emptyRDD() – Creates an empty RDD

getOrCreate() – Creates or returns a SparkContext

hadoopFile() – Returns an RDD of a Hadoop file

newAPIHadoopFile() – Creates an RDD for a Hadoop file with a new API InputFormat.

sequenceFile() – Get an RDD for a Hadoop SequenceFile with given key and value types.

setLogLevel() – Change log level to debug, info, warn, fatal, and error

textFile() – Reads a text file from HDFS, local or any Hadoop supported file systems and returns an RDD

union() – Union two RDDs

wholeTextFiles() – Reads a text file in the folder from HDFS, local or any Hadoop supported 
file systems and returns an RDD of Tuple2. The first element of the tuple consists file name 
and the second element consists context of the text file.
'''