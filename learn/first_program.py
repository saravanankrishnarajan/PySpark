from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Example").getOrCreate()

df = spark.read.csv("data/bank-data.csv",header=True,inferSchema=True)

df.printSchema()

df.show(10)

print("Number of rows:", df.count())

selected_df = df.select("marital","age")

aggregated_df_count = selected_df.groupby("marital").agg({"age":"count"})

aggregated_df_count.show()

aggregated_df_age_mean = selected_df.groupby("marital").agg({"age":"mean"})

aggregated_df_age_mean.show()

spark.stop()