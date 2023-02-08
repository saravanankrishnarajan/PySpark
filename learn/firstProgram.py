from pyspark  import SparkContext

sc = SparkContext()

logFile ="file:///D:/BigDataLocalSetup/spark-3.2.3-bin-hadoop3.2/README.md"
logData = sc.textFile(logFile).cache()

#print(logData)

numAs = logData.filter(lambda s: 'a' in s).count()
print(numAs)
numBs = logData.filter(lambda s: 'b' in s).count()
print("lines with a: %i, lines with b: %i" %(numAs,numBs))
sc.stop()