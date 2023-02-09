from pyspark import SparkContext

sc = SparkContext('local','group by transformation')

rdd = sc.parallelize([("A",1),("B",2),("A",3),("C",4),("B",5)])

groupd_rdd = rdd.groupByKey()

result = groupd_rdd.collect()

for key,value in result:
    print(key,list(value))

result2 = sc.parallelize(result)
result2.saveAsTextFile("hdfs://localhost:9000/transformation/group_by_transformation.txt")

print(result2)