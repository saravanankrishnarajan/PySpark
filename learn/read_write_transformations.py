from pyspark import SparkContext

sc = SparkContext("local","Read and Write into hdfs")

rdd1 = sc.textFile("hdfs://localhost:9000/LocalStorage/infile1.txt")
rdd2 = sc.textFile("hdfs://localhost:9000/LocalStorage/infile2.txt")

print("rdd1: ",rdd1.collect())
print("rdd2",rdd2.collect())

intersection_rdd = rdd1.intersection(rdd2)

print(intersection_rdd.collect())

intersection_rdd.saveAsTextFile("hdfs://localhost:9000/input_dir/intersect_transformation.txt")
sc.stop()
