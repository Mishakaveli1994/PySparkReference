# Get current active SparkContext and configuration

'''
In Spark/PySpark you can get the current active SparkContext and its configuration
settings by accessing spark.sparkContext.getConf.getAll(), here spark is an object of
SparkSession and getAll() returns Array[(String, String)], let’s see with examples using
Spark with Scala & PySpark (Spark with Python).
'''

'''
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

configurations = spark.sparkContext.getConf().getAll()
for item in configurations: print(item)


This prints the below configuration. Alternatively, you can also get the PySpark configurations using spark.sparkContext._conf.getAll()


('spark.app.name', 'SparkByExamples.com')
('spark.rdd.compress', 'True')
('spark.driver.host', 'DELL-ESUHAO2KAJ')
('spark.serializer.objectStreamReset', '100')
('spark.submit.pyFiles', '')
('spark.executor.id', 'driver')
('spark.submit.deployMode', 'client')
('spark.app.id', 'local-1617974806929')
('spark.ui.showConsoleProgress', 'true')
('spark.master', 'local[1]')
('spark.driver.port', '65211')
'''

# Get Specific Configuration
'''print(spark.sparkContext.getConf().get("spark.driver.host"))'''
