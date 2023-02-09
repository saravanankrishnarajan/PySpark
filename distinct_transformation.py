from pyspark import SparkContext

sc = SparkContext('local', 'distinct transformation')

rdd1 = sc.parallelize([1 , 2 , 3 , 4 , 5 , 6])
rdd2 = sc.parallelize([5 , 6 , 7 , 8 , 9])

rdd3 = rdd1.union(rdd2)

distinct_rdd = rdd3.distinct()


result = distinct_rdd.collect()

print(result)

result_par = sc.parallelize(result)
result_par.saveAsTextFile("hdfs://localhost:9000/transformation/distinct_transformation.txt")

sc.stop()

