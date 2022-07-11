'''PySpark SQL is one of the most used PySpark modules which is used for processing structured columnar data format. Once you have a DataFrame created, you can interact with the data by using SQL syntax.

In other words, Spark SQL brings native RAW SQL queries on Spark meaning you can run traditional ANSI SQL’s on Spark Dataframe, in the later section of this PySpark SQL tutorial, you will learn in detail using SQL select, where, group by, join, union e.t.c

In order to use SQL, first, create a temporary table on DataFrame using createOrReplaceTempView() function. Once created, this table can be accessed throughout the SparkSession using sql() and it will be dropped along with your SparkContext termination.

Use sql() method of the SparkSession object to run the query and this method returns a new DataFrame.'''

# df.createOrReplaceTempView("PERSON_DATA")
# df2 = spark.sql("SELECT * from PERSON_DATA")
# df2.printSchema()
# df2.show()

'''GroupBy'''
# groupDF = spark.sql("SELECT gender, count(*) from PERSON_DATA group by gender")
# groupDF.show()
