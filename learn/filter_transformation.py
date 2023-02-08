from pyspark import SparkContext

sc = SparkContext("local","Filter transformation")

numbers=[1,2,3,4,5,6,7,8,9,10]
rdd = sc.parallelize(numbers)

even_rdd = rdd.filter(lambda x: x % 2 == 0 )

result = even_rdd.collect()

print(result)

sc.stop()