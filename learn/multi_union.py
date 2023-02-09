from pyspark import SparkContext

sc = SparkContext("local","Read and Write into hdfs")


rdd1 = sc.parallelize([1,2,3,4])
rdd2 = sc.parallelize([5,6,7,8])

union_rdd = rdd1.union(rdd2)
print(union_rdd.collect())

rdd3 = sc.parallelize([9,10,11,12])

union_rdd = rdd1.union(rdd2).union(rdd3)

print(union_rdd.collect())
union_rdd.saveAsTextFile("hdfs://localhost:9000/input_dir/multi_union_transformation.txt")
sc.stop()
