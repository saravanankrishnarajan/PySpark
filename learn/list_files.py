from pyspark import SparkContext

sc = SparkContext("local", "Hadoop Files Listing Example")

hdfs_files = sc.wholeTextFiles("/")

for file_name, _ in hdfs_files.collect():
    print("File Name:", file_name)
