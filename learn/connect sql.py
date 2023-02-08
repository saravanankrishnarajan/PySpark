from pyspark import SparkContext

import mysql.connector

db = mysql.connector.connect(
    host = "localhost",
    user = "hive",
    password="hive",
    database="employee"
)
cursor = db.cursor()

# Execute SQL query
cursor.execute("SELECT * FROM employee")

# Fetch results
results = cursor.fetchall()

# Close cursor and connection
cursor.close()


# Create a SparkContext
sc = SparkContext("local", "Write RDD to HDFS")

# Convert results to RDD
rdd = sc.parallelize(results)

print(rdd.collect())

sc.stop()