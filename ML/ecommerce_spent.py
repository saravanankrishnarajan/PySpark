import findspark
print(findspark.init())
from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegression
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler

spark = SparkSession.builder.appName('Ecommerce spent').getOrCreate()
print('\n')
print("Loading Ecomerrce customers files from csv file")
data = spark.read.csv("Ecommerce_Customers.csv",inferSchema=True, header=True)

print('\n')
print("Example record from the dataframe")
for i in data.head(5)[1]:
    print(i)
print('\n')

print("Assembling the numeric cols using Vector assembler")
VAssmebler = VectorAssembler(inputCols = ['Avg Session Length', 'Time on App', 'Time on Website', 'Length of Membership'],outputCol='features')
output = VAssmebler.transform(data)
print('\n')
print("After numeric vector assembled, all numeric values are assembled as features")
print(output.head(1))
print('\n')

final_data = output.select('features','Yearly Amount Spent')
train_data, test_data = final_data.randomSplit([0.7, 0.3])
print('\n')

print("train data count:",train_data.count())
print("test data count:",test_data.count())
print('\n')

LR = LinearRegression(labelCol='Yearly Amount Spent')
LRModel = LR.fit(train_data)

test_results = LRModel.evaluate(test_data)

print("Top five predictions of test data")
for i in test_results.predictions.head(5):
    print(i)

print('\n')
print('\n')
print("test data prediction RMSE:",test_results.rootMeanSquaredError)
print("Test data r2 value",test_results.r2)
