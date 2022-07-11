from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

spark = SparkSession.builder.appName('pyspark-by-examples').getOrCreate()

'''
Spark pivot() function is used to pivot/rotate the data from one DataFrame/Dataset column into 
multiple columns (transform row to column) and unpivot is used to transform it back (transform columns to rows).
'''

data = [("Banana", 1000, "USA"), ("Carrots", 1500, "USA"), ("Beans", 1600, "USA"),
        ("Orange", 2000, "USA"), ("Orange", 2000, "USA"), ("Banana", 400, "China"),
        ("Carrots", 1200, "China"), ("Beans", 1500, "China"), ("Orange", 4000, "China"),
        ("Banana", 2000, "Canada"), ("Carrots", 2000, "Canada"), ("Beans", 2000, "Mexico")]

df = spark.createDataFrame(data=data, schema=["Product", "Amount", "Country"])
df.printSchema()
df.show(truncate=False)

'''Pivot Dataframe'''
print('Pivot Dataframe')
pivot_df = df.groupBy('Product').pivot('Country').sum('Amount')
pivot_df.show()

'''Spark 2.0 on-wards performance has been improved on Pivot, 
however, if you are using lower version; note that pivot is a 
very expensive operation hence, it is recommended to provide column data 
(if known) as an argument to function as shown below.'''

# Option 1
countries = ["USA", "China", "Canada", "Mexico"]
pivot_df_option1 = df.groupBy("Product").pivot("Country", countries).sum("Amount")
pivot_df_option1.show(truncate=False)

# Option 2 (two-phase aggregation)
pivot_df_option2 = df.groupBy("Product", "Country") \
    .sum("Amount") \
    .groupBy("Product") \
    .pivot("Country") \
    .sum("sum(Amount)")
pivot_df_option2.show(truncate=False)

'''Unpivot DataFrame'''
print('Unpivot DataFrame')
unpivotExpr = "stack(3, 'Canada', Canada, 'China', China, 'Mexico', Mexico) as (Country,Total)"
unPivotDF = pivot_df.select("Product", expr(unpivotExpr)) \
    .where("Total is not null")
unPivotDF.show(truncate=False)