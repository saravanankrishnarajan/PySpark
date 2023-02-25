#!/usr/bin/env python
# coding: utf-8

# ### Module 6: Dataframes and Spark SQL

# #### Case Study: Mobile App Store
# #### Domain: Telecom

# In[ ]:

# In[36]:


import findspark
findspark.init()

import os
from pyspark.sql import SparkSession, SQLContext


# In[37]:


from pyspark.sql.functions import explode
from pyspark.sql.functions import countDistinct, avg
from pyspark.sql.functions import dayofmonth, dayofyear, year, month, hour, weekofyear, date_format
from pyspark.sql.functions import col as func_col

import warnings
warnings.filterwarnings('ignore')
# In[38]:


#1. Load data into Spark DataFrame
print('*'*100)
print('1. Load data into Spark DataFrame')
print('*'*100)


# In[39]:


spark = SparkSession.builder.appName("Module6CaseStudy2").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = df = spark.read.option("encoding", "windows-1252").csv('651_cs2_datasets_v1.0/access.log',inferSchema=True,sep=' ')
df.printSchema()


# In[40]:


name_list = ['remote_host', '_c1', '_c2', 'request_time', 'request_zone', 'request_type', 'url', 'web_version', 'status',
 'bytes_setnt', '_c10', 'UA1', 'UA2', 'UA3', 'UA4', 'UA5', 'UA6', 'UA7', '_c18']
df = df.toDF(*name_list)


# In[41]:


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


# In[42]:


df = df.replace('-',None,['_c1','_c2','_c10','_c18'])


# In[46]:


spark.catalog.dropTempView('Access')


# In[47]:


df.createTempView('Access')


# In[48]:


spark.sql("select * from Access limit 10").show()


# In[ ]:


# 2. Find out how many 404 HTTP codes are in access logs.
print()
print('*'*100)
print('2. Find out how many 404 HTTP codes are in access logs.')
print('*'*100)

# In[49]:


spark.sql("select count(status) from Access where status='404'").show()


# In[50]:


# 3. Find out which URLs are broken.
print()
print('*'*100)
print('3. Find out which URLs are broken.')
print('*'*100)


# In[51]:


spark.sql("select count(url) from Access where length(url)=0").show()


# In[ ]:


# 4. Verify there are no null columns in the original dataset.
print()
print('*'*100)
print('4. Verify there are no null columns in the original dataset.')
print('*'*100)


# In[14]:


#select count(isnan())


# In[60]:


from pyspark.sql.functions import col

spark.sql("""select sum(if(remote_host is Null, 1,0)) as remote_host,
                  sum(if(_c1 is Null, 1,0)) _c1,
                  sum(if(_c2 is Null, 1,0)) _c2,
                  sum(if(Time is Null, 1,0)) Time,
                  sum(if(Type is Null, 1,0)) Type,
                  sum(if(URL is Null, 1,0)) URL,
                  sum(if(Web is Null, 1,0)) Web,
                  sum(if(status is Null, 1,0)) status,
                  sum(if(Bytes is Null, 1,0)) Bytes,
                  sum(if(_c10 is Null, 1,0)) _c10,
                  sum(if(UserAgent is Null, 1,0)) UserAgent,
                  sum(if(_c18 is Null, 1,0)) _c18
          from Access""").show()


# In[ ]:


# 5. Replace null values with constants such as 0
print()
print('*'*100)
print('5. Replace null values with constants such as 0')
print('*'*100)


# In[62]:


df = df.fillna({'_c1':0,'_c2':0, '_c10':0, '_c18':0, 'web':0})


# In[64]:


spark.catalog.dropTempView('Access')


# In[65]:


df.createTempView('Access')


# In[66]:


spark.sql("""select sum(if(remote_host is Null, 1,0)) as remote_host,
                  sum(if(_c1 is Null, 1,0)) _c1,
                  sum(if(_c2 is Null, 1,0)) _c2,
                  sum(if(Time is Null, 1,0)) Time,
                  sum(if(Type is Null, 1,0)) Type,
                  sum(if(URL is Null, 1,0)) URL,
                  sum(if(Web is Null, 1,0)) Web,
                  sum(if(status is Null, 1,0)) status,
                  sum(if(Bytes is Null, 1,0)) Bytes,
                  sum(if(_c10 is Null, 1,0)) _c10,
                  sum(if(UserAgent is Null, 1,0)) UserAgent,
                  sum(if(_c18 is Null, 1,0)) _c18
          from Access""").show()


# In[ ]:


# 6. Parse timestamp to readable date.
print()
print('*'*100)
print('6. Parse timestamp to readable date.')
print('*'*100)


# In[82]:


spark.sql("select to_timestamp(substring(Time,2,26) ,'dd/MMM/yyyy:HH:mm:ss x') Date_Parsed from Access").show(truncate=False)


# In[ ]:


# 7. Describe which HTTP status values appear in data and how many.
print()
print('*'*100)
print('7. Describe which HTTP status values appear in data and how many.')
print('*'*100)


# In[85]:


spark.sql("SELECT status,count(status) from Access group by status").show()


# In[ ]:


# 8. How many unique hosts are there in the entire log and their average request
print()
print('*'*100)
print('8. How many unique hosts are there in the entire log and their average request')
print('*'*100)


# In[87]:


spark.sql("select count(distinct(remote_host)) from Access").show()


# In[94]:


spark.sql("select avg(cnt) from (select remote_host,count(remote_host) cnt from Access  group by remote_host) t").show()


# In[95]:


spark.sql("select count(*) from Access").show()


# In[96]:


print(2338006/40836)


# In[ ]:


# 9. Create a spark-submit application for the same and print the findings in the log

