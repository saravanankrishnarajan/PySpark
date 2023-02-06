import findspark
from pyspark import SparkConf
from pyspark.sql import SparkSession

findspark.init()
findspark.find()

conf = SparkConf().setAppName("Hadoop Hostname and Port")

spark = SparkSession.builder.config(conf=conf).getOrCreate()

hadoop_config = spark.sparkContext._jsc.hadoopConfiguration()

hostname = hadoop_config.get("dfs.namenode.rpc-address")

#hostname = hadoop_config.get("fs.defaultFS")


print(hostname)