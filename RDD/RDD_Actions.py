'''
RDD actions are PySpark operations that return the values to the driver program.
Any function on RDD that returns other than RDD is considered as an action in PySpark programming.
In this tutorial, I will explain the most used RDD actions with examples.
'''

from pyspark.sql import SparkSession
from operator import add

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
data = [("Z", 1), ("A", 20), ("B", 30), ("C", 40), ("B", 30), ("B", 60)]
inputRDD = spark.sparkContext.parallelize(data)

listRdd = spark.sparkContext.parallelize([1, 2, 3, 4, 5, 3, 2])

'''Aggregate'''
'''
Prototype:

aggregate(zeroValue, seqOp, combOp)

Description:

aggregate() lets you take an RDD and generate a single value that is of a different type than what was stored
 in the original RDD.

Parameters:

zeroValue: The initialization value, for your result, in the desired format.
seqOp: The operation you want to apply to RDD records. Runs once for every record in a partition.
combOp: Defines how the resulted objects (one for every partition), gets combined.
Example:

Compute the sum of a list and the length of that list. Return the result in a pair of (sum, length).

In a Spark shell, I first created a list with 4 elements, with 2 partitions:

listRDD = sc.parallelize([1,2,3,4], 2)
then I defined my seqOp:

seqOp = (lambda local_result, list_element: (local_result[0] + list_element, local_result[1] + 1) )
and my combOp:

combOp = (lambda some_local_result, another_local_result: (some_local_result[0] + another_local_result[0],
 some_local_result[1] + another_local_result[1]) )
and then I aggregated:

listRDD.aggregate( (0, 0), seqOp, combOp)
Out[8]: (10, 4)
As you can see, I gave descriptive names to my variables, but let me explain it further:

The first partition has the sublist [1, 2]. We will apply the seqOp to each element of that list and this
 will produce a local result, a pair of (sum, length), that will reflect the result locally, only in that first partition.

So, let's start: local_result gets initialized to the zeroValue parameter we provided the aggregate() with,
 i.e. (0, 0) and list_element is the first element of the list, i.e. 1. As a result this is what happens:

0 + 1 = 1
0 + 1 = 1
Now, the local result is (1, 1), that means, that so far, for the 1st partition, after processing only the first 
element, the sum is 1 and the length 1. Notice, that local_result gets updated from (0, 0), to (1, 1).

1 + 2 = 3
1 + 1 = 2
and now the local result is (3, 2), which will be the final result from the 1st partition, since they are no 
other elements in the sublist of the 1st partition.

Doing the same for 2nd partition, we get (7, 2).

Now we apply the combOp to each local result, so that we can form, the final, global result, 
like this: (3,2) + (7,2) = (10, 4)

Example described in 'figure':

seqOp = (lambda local_result, list_element: (local_result[0] + list_element, local_result[1] + 1) )
Explanation: 
    (local_result[0] + list_element = (0, 0)[0] + (1, 2)[0] = 0 + 1 = 1
    local_result[1] + 1 = (0, 0)[1] + 1 = 0 + 1 = 1
    (local_result[0] + list_element = (1, 1)[0] + (1, 2)[1] = 2 + 1 = 3
    local_result[1] + 1 = (1, 1)[1] + 1 = 1 + 1 = 2
    First Partition = (3, 2)

Local
            (0, 0) <-- zeroValue

[1, 2]                  [3, 4]

0 + 1 = 1               0 + 3 = 3
0 + 1 = 1               0 + 1 = 1

1 + 2 = 3               3 + 4 = 7
1 + 1 = 2               1 + 1 = 2       
    |                       |
    v                       v
  (3, 2)                  (7, 2)
      \                    / 
       \                  /
        \                /
         \              /
          \            /
           \          / 
           ------------
           |  combOp  |
           ------------
                |
                v
             (10, 4)
Inspired by this great example.

So now if the zeroValue is not (0, 0), but (1, 0), one would expect to get (8 + 4, 2 + 2) = (12, 4), which doesn't 
explain what you experience. Even if we alter the number of partitions of my example, I won't be able to get that again.

The key here is JohnKnight's answer, which state that the zeroValue is not only analogous to the number of 
partitions, but may be applied more times than you expect.
'''

print('Aggregate Example 1')
seqOp = (lambda x, y: x + y)
combOp = (lambda x, y: x + y)
agg = listRdd.aggregate(0, seqOp, combOp)
print(agg)  # output 20

print('Aggregate Example 2')
# aggregate 2
seqOp2 = (lambda x, y: (x[0] + y, x[1] + 1))  # => sum(listRdd), count(listRdd)
combOp2 = (lambda x, y: (x[0] + y[0], x[1] + y[1]))
agg2 = listRdd.aggregate((0, 0), seqOp2, combOp2)
print(agg2)  # output (20,7)

'''treeAggregate
treeAggregate() – Aggregates the elements of this RDD in a multi-level tree pattern. 
The output of this function will be similar to the aggregate function.

Syntax: treeAggregate(zeroValue, seqOp, combOp, depth=2)
'''
print('treeAggregate')
seqOp = (lambda x, y: x + y)
combOp = (lambda x, y: x + y)
agg = listRdd.treeAggregate(0, seqOp, combOp)
print(agg)  # output 20

'''fold
Aggregate the elements of each partition, and then the results for all the partitions.
'''
print('Fold')
foldRes = listRdd.fold(0, add)
print(foldRes)

'''reduce
Reduces the elements of the dataset using the specified binary operator.
'''
print('Reduce')
redRes = listRdd.reduce(add)
print(redRes)  # output 20

'''treeReduce
Reduces the elements of this RDD in a multi-level tree pattern.
'''
print('TreeReduce')
add = lambda x, y: x + y
redRes = listRdd.treeReduce(add)
print(redRes)  # output 20

'''collect
Return the complete dataset as an Array.
'''
print('Collect')
data = listRdd.collect()
print(data)

'''count'''
# Return the count of elements in the dataset.
print("Count : " + str(listRdd.count()))
# Output: Count : 20

'''countApprox'''
# Return approximate count of elements in the dataset, this method returns incomplete
# when execution time meets timeout.
print("countApprox : " + str(listRdd.countApprox(1200)))
# Output: countApprox : (final: [7.000, 7.000])

'''countApproxDistinct'''
# Return an approximate number of distinct elements in the dataset.
print("countApproxDistinct : " + str(listRdd.countApproxDistinct()))
# Output: countApproxDistinct : 5

print("countApproxDistinct : " + str(inputRDD.countApproxDistinct()))
# Output: countApproxDistinct : 5

'''countByValue'''
# Return Map[T,Long] key representing each unique value in dataset and value represents count each value present.
print("countByValue :  " + str(listRdd.countByValue()))

'''first'''
# Return the first element in the dataset.
# first
print("first :  " + str(listRdd.first()))
# Output: first :  1
print("first :  " + str(inputRDD.first()))
# Output: first :  (Z,1)

'''top'''
# Return top n elements from the dataset.
# top
print("top : " + str(listRdd.top(2)))
# Output: take : 5,4
print("top : " + str(inputRDD.top(2)))
# Output: take : (Z,1),(C,40)

'''min'''
# Return the minimum value from the dataset.
# min
print("min :  " + str(listRdd.min()))
# Output: min :  1
print("min :  " + str(inputRDD.min()))
# Output: min :  (A,20)

'''max'''
# Return the maximum value from the dataset.
# max
print("max :  " + str(listRdd.max()))
# Output: max :  5
print("max :  " + str(inputRDD.max()))
# Output: max :  (Z,1)

'''take'''
# Return the first num elements of the dataset.
print("take : " + str(listRdd.take(2)))
# Output: take : 1,2

'''takeOrdered'''
# Return the first num (smallest) elements from the dataset and this is the opposite of the take() action.
# Note: Use this method only when the resulting array is small, as all the data is loaded into the driver’s memory.
print("takeOrdered : " + str(listRdd.takeOrdered(2)))
# Output: takeOrdered : 1,2

'''takeSample'''
# Return the subset of the dataset in an Array.
# Note: Use this method only when the resulting array is small, as all the data is loaded into the driver’s memory.
print("take : " + str(listRdd.takeSample(False, 5, 2)))
