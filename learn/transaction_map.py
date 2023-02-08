from pyspark import SparkContext

sc = SparkContext("local","Map Transaction")

numbers=[1,2,3,4,5,6]

rdd = sc.parallelize(numbers)

squared_rdd = rdd.map(lambda x: x**2)

result = squared_rdd.collect()

print(result)

sc.stop()
