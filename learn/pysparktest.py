import pyspark

spark = pyspark.sql.SparkSession.builder.appName("MyApp").getOrCreate()

print("pyspark version:", spark.version)