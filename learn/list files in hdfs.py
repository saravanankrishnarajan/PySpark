from pyspark import SparkContext
from pyspark.sql import SparkSession

sc = SparkContext("local","list files  in hdfs")

spark = SparkSession(sc)

df = spark.read.text("hdfs://localhost:9000/input_dir")
print(df.show())

sc.stop()
