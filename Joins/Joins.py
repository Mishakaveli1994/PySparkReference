'''
join(self, other, on=None, how=None)
Scala
join() operation takes parameters as below and returns DataFrame.

param other: Right side of the join
param on: a string for the join column name
param how: default inner. Must be one of inner, cross, outer,full, full_outer,
left, left_outer, right, right_outer,left_semi, and left_anti.

You can also write Join expression by adding where() and filter()
methods on DataFrame and can have Join on multiple columns.
'''

'''
Pyspark Join Types

------------Join String--------------Equivalent SQL Join
               inner	          |       INNER JOIN    |
outer, full, fullouter, full_outer|	   FULL OUTER JOIN  |
left, leftouter, left_outer	      |       LEFT JOIN     |
right, rightouter, right_outer	  |       RIGHT JOIN    |
cross	                          |                     | 
anti, leftanti, left_anti	      |                     |
semi, leftsemi, left_semi	      |                     |
---------------------------------------------------------
'''

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

emp = [(1, "Smith", -1, "2018", "10", "M", 3000),
       (2, "Rose", 1, "2010", "20", "M", 4000),
       (3, "Williams", 1, "2010", "10", "M", 1000),
       (4, "Jones", 2, "2005", "10", "F", 2000),
       (5, "Brown", 2, "2010", "40", "", -1),
       (6, "Brown", 2, "2010", "50", "", -1)
       ]
empColumns = ["emp_id", "name", "superior_emp_id", "year_joined",
              "emp_dept_id", "gender", "salary"]

empDF = spark.createDataFrame(data=emp, schema=empColumns)
empDF.printSchema()
empDF.show(truncate=False)

dept = [("Finance", 10),
        ("Marketing", 20),
        ("Sales", 30),
        ("IT", 40)
        ]
deptColumns = ["dept_name", "dept_id"]
deptDF = spark.createDataFrame(data=dept, schema=deptColumns)
deptDF.printSchema()
deptDF.show(truncate=False)

'''Inner Join
Inner join is the default join in PySpark and it’s mostly used. 

This joins two datasets on key columns, where keys don’t match the rows get 
dropped from both datasets (emp & dept).'''
print('Inner Join')
empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, "inner") \
    .show(truncate=False)

'''Full Outer Join
Outer a.k.a full, fullouter join returns all rows from both datasets, 
where join expression doesn't match it returns null on respective record columns.

From our “emp” dataset’s “emp_dept_id” with value 50 doesn’t have a record on “dept” 
hence dept columns have null and “dept_id” 30 doesn't have a record in “emp” hence you 
see null’s on emp columns.'''
print('Full Outer Join')
empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, "outer") \
    .show(truncate=False)

empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, "full") \
    .show(truncate=False)

empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, "fullouter") \
    .show(truncate=False)

'''Left Outer Join
Left a.k.a Leftouter join returns all rows from the left dataset regardless of 
match found on the right dataset when join expression doesn’t match, it assigns 
null for that record and drops records from right where match not found.

From our dataset, “emp_dept_id” 5o doesn't have a record on “dept” dataset hence, 
this record contains null on “dept” columns (dept_name & dept_id). and “dept_id” 30 
from “dept” dataset dropped from the results.'''
print('Left Outer Join')
empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, "left") \
    .show(truncate=False)

empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, "leftouter") \
    .show(truncate=False)

'''Right Outer Join
Right a.k.a Rightouter join is opposite of left join, here it 
returns all rows from the right dataset regardless of math 
found on the left dataset, when join expression doesn’t match, it assigns 
null for that record and drops records from left where match not found.

From our example, the right dataset “dept_id” 30 
doesn't have it on the left dataset “emp” hence, this record 
contains null on “emp” columns. and “emp_dept_id” 50 dropped as a 
match not found on left.

'''
print('Right Outer Join')
empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, "right") \
    .show(truncate=False)

empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, "rightouter") \
    .show(truncate=False)

'''Left Semi Join
leftsemi join is similar to inner join difference being leftsemi join returns 
all columns from the left dataset and ignores all columns from the right dataset. 
In other words, this join returns columns from the only left dataset for the records 
match in the right dataset on join expression, records not matched on join expression are 
ignored from both left and right datasets.

The same result can be achieved using select on the result of the inner join however, 
using this join would be efficient.'''
print('Left Semi Join')
empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, "leftsemi") \
    .show(truncate=False)

'''Left Anti Join
leftanti join does the exact opposite of the leftsemi, leftanti join 
returns only columns from the left dataset for non-matched records.
'''
print('Left Anti Join')
empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, "leftanti") \
    .show(truncate=False)

'''Self Join
Joins are not complete without a self join, Though there is no self-join type 
available, we can use any of the above-explained join types to join DataFrame to itself. 
Below example use inner self join.'''
print('Self Join')
empDF.alias("emp1").join(empDF.alias("emp2"),
                         col("emp1.superior_emp_id") == col("emp2.emp_id"), "inner") \
    .select(col("emp1.emp_id"), col("emp1.name"),
            col("emp2.emp_id").alias("superior_emp_id"),
            col("emp2.name").alias("superior_emp_name")) \
    .show(truncate=False)

'''Using SQL Expression
Since PySpark SQL support native SQL syntax, we can also write join operations after creating 
temporary tables on DataFrames and use these tables on spark.sql().'''
print('Using SQL Expression')
empDF.createOrReplaceTempView("EMP")
deptDF.createOrReplaceTempView("DEPT")

joinDF = spark.sql("select * from EMP e, DEPT d where e.emp_dept_id == d.dept_id")
joinDF.show(truncate=False)

joinDF2 = spark.sql("select * from EMP e INNER JOIN DEPT d ON e.emp_dept_id == d.dept_id")
joinDF2.show(truncate=False)

'''
PySpark SQL Join on multiple DataFrames
When you need to join more than two tables, you either use SQL 
expression after creating a temporary view on the DataFrame or use the result of 
join operation to join with another DataFrame like chaining them. for example

df1.join(df2,df1.id1 == df2.id2,"inner") \
   .join(df3,df1.id1 == df3.id3,"inner")
'''
