import findspark
from pyspark import SparkConf
from pyspark.sql import SparkSession

findspark.init()
findspark.find()

conf = SparkConf().setAppName("Hadoop Storage access example")

spark = SparkSession.builder.config(conf=conf).getOrCreate()

hadoop_config = spark.sparkContext._jsc.hadoopConfiguration()

hdfs_path = hadoop_config.get("fs.defaultFS")

print("HDFS path:",hdfs_path)