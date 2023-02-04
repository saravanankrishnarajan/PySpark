from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Example").getOrCreate()

df = spark .read.csv("departments.csv",header=True,inferSchema=True)

df.printSchema()

df.show(10)

print("Number of rows:", df.count())

selecteddf.select("department")