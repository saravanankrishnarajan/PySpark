from pyspark import SparkContext

sc = SparkContext("local","Union Transformation")

rdd1 = sc.parallelize([1,2,3,4,5])
rdd2 = sc.parallelize([5,4,6,7,8])

union_rdd = rdd1.union(rdd2)

result = union_rdd.collect()
print(result)

hdfs_write = sc.parallelize(result)

hdfs_write.saveAsTextFile("hdfs://localhost:9000/transformation/union_transformation")

sc.stop()


