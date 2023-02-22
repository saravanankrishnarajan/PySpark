import findspark
print(findspark.init())

from pyspark.sql import SparkSession
import numpy as np

spark = SparkSession.builder.appName('1stPysparkML').getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

from pyspark.ml.regression import LinearRegression

all_data  = spark.read.format('libsvm').load('sample_linear_regression_data.txt')

print("Loaded libsvm file and printing header")
print(all_data.show(5))

print("splitting training and test datas")

train_data, test_data = all_data.randomSplit([0.7, 0.3])

print("creating Linear regression model")
lr = LinearRegression(featuresCol='features' , labelCol='label' , predictionCol='predictions')

print("Training data count: " , train_data.count())
print("Training data count: " , test_data.count())

print("Training the model")
Model = lr.fit(train_data)

print("Test the model with test data")
test_results = Model.evaluate(test_data)

print("Root mean squared error is" , test_results.rootMeanSquaredError)

train_features=test_data.select("features")
predictions = Model.transform(test_data)

print(predictions.show(5))






