'''
Default behavior is to generate part files and name can't be configured

val df = Seq("one", "two", "three").toDF("num")

repartition property configures how many part files are generated

df.repartition(3).write.csv(sys.env("HOME")+ "/Documents/tmp/some-files") - 3 files
df.repartition(1).write.csv(sys.env("HOME")+ "/Documents/tmp/one-file-repartition") - 1 file


coalesce property genereates one file, but you lose parallelism

df.coalesce(1).write.csv(sys.env("HOME")+ "/Documents/tmp/one-file-coalesce") - 1 file

Writing out a file with a specific name - spark-daria module

Example:
import com.github.mrpowers.spark.daria.sql.DariaWriters
DariaWriters.writeSingleFile(
    df = df,
    format = "csv",
    sc = spark.sparkContext,
    tmpFolder = sys.env("HOME") + "/Documents/better/tmp",
    filename = sys.env("HOME") + "/Documents/better/mydata.csv"
)
'''