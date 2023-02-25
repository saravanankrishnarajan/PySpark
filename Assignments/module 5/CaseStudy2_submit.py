import findspark
findspark.init()

import pyspark 
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Apple").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

import warnings
warnings.filterwarnings('ignore')

# 1. Load file as a text file in spark
print("1. Load file as a text file in spark")
df = spark.read.option("encoding", "windows-1252").csv('bjupm6tr0qi/651_m5_cs2_datasets_v1.0',inferSchema=True,sep=' ')
df.printSchema()
df.show(1,truncate=False)
df.select()
name_list = ['remote_host',
 '_c1',
 '_c2',
 'request_time',
 'request_zone',
 'request_type',
 'url',
 'web_version',
 'status',
 'bytes_setnt',
 '_c10',
 'UA1',
 'UA2',
 'UA3',
 'UA4',
 'UA5',
 'UA6',
 'UA7',
 '_c18']
df = df.toDF(*name_list)

df.show(1)

from pyspark.sql.functions import concat,concat_ws
df = df.select("remote_host",
               '_c1',
               '_c2',
          concat_ws(' ',df['request_time'],df['request_zone']).alias('Time'),
          df['request_type'].alias('Type'),
         df['url'].alias('URL'),
         df['web_version'].alias('Web'),
         df['status'],
         df['bytes_setnt'].alias('Bytes'),
               '_c10',
         concat_ws(' ','UA1','UA2','UA3','UA4','UA5','UA6','UA7').alias('UserAgent'),
              '_c18')

df.show(10)

df = df.replace('-',None,['_c1','_c2','_c10','_c18'])



# 2. Find out how many 404 HTTP codes are in access logs.
print("*"*100)
print("2. Find out how many 404 HTTP codes are in access logs.")
print("*"*100)
print(df.filter('status==400').count())

# 3. Find out which URLs are broken.
print("*"*100)
print("3. Find out which URLs are broken.")
print("*"*100)

from pyspark.sql.functions import length
print(df.filter(length(df['url'])==0).count())


#4. Verify there are no null columns in the original dataset.
print("*"*100)
print("4. Verify there are no null columns in the original dataset.")
print("*"*100)
from pyspark.sql.functions import count,when,isnan,col
df.select([count(when(isnan(c)|col(c).isNull(),c)).alias(c) for c in df.columns]).show()
print(df.count())
print("There are 4 columns we can consider as cannot be used. We candrop them.")

# 5. Replace null values with constants such as 0
print("*"*100)
print("5. Replace null values with constants such as 0")
print("*"*100)
df = df.fillna({'_c1':0,'_c2':0, '_c10':0, '_c18':0, 'web':0})
df.select([count(when(isnan(c)|col(c).isNull(),c)).alias(c) for c in df.columns]).show()


# 6. Parse timestamp to readable date.
print("*"*100)
print("6. Parse timestamp to readable date.")
print("*"*100)

from pyspark.sql.functions import date_format,to_timestamp,substring,expr
df.select(to_timestamp(expr("substring(Time, 2, length(Time)-2)"),'dd/MMM/yyyy:HH:mm:ss x').alias("Parsed_Date")).show(truncate=False)

# 7. Describe which HTTP status values appear in data and how many.
print("*"*100)
print("7. Describe which HTTP status values appear in data and how many.")
print("*"*100)
plot_df = df.groupBy('status').count()
plot_df.show()

# 8. Display as chart the above stat in chart in Zeppelin notebook
print("*"*100)
print("8. Display as chart the above stat in chart in Zeppelin notebook")
print("*"*100)
import matplotlib.pyplot as plt
import pyspark.pandas as ps

plot_pd = plot_df.toPandas()

plot_pd.plot.bar(x='status',y='count')

# 9. How many unique hosts are there in the entire log and their average request
print("*"*100)
print("9. How many unique hosts are there in the entire log and their average request")
print("*"*100)
from pyspark.sql.functions import countDistinct

distinct_count = df.select(countDistinct('remote_host')).collect()[0][0]
print("Distinct hosts count",distinct_count)


dist_df = df.groupBy('remote_host').agg(count('remote_host').alias('cnt'))
from pyspark.sql.functions import avg
dist_df.select(avg('cnt')).show()