from pyspark import SparkContext

sc = SparkContext("local","flatmap transformation")

sentences = ["Hello World!","How are you?","I'm fine, thank you."]
rdd = sc.parallelize(sentences)

words_rdd = rdd.flatMap(lambda x: x.split(" "))

result = words_rdd.collect()

print(result)

par_result = sc.parallelize(result)

par_result.saveAsTextFile("hdfs://localhost:9000/transformation/flatmap_transformation")

sc.stop()
