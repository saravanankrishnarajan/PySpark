import findspark
from pyspark.sql import SparkSession

findspark.init()
findspark.find()

spark = SparkSession.builder.appName("Example2").getOrCreate()

df = spark.createDataFrame([(1, "john", 25), (2, "Jane", 30), (3, "Jim", 35)], ["id", "name", "age"])

df.show()

spark.stop()

print(findspark.find())
