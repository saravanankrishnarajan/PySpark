import findspark
from pyspark import SparkContext

findspark.init()
findspark.find()

sc = SparkContext("local","File writing")

data = sc.parallelize(["line 1", "Line 2", "Line 3"])

data.saveAsTextFile("file:///test23.txt")