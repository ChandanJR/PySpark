# -*- coding: utf-8 -*-
"""
Created on Mon Jan 10 14:03:04 2022

@author: Asus
"""

#initialization of Spark Session

from random import random
import os
from pyspark.sql import SparkSession
os.environ['SPARK_HOME'] = "C:/Users/Asus/Desktop/BDA SEM 3/spark/spark-3.2.0-bin-hadoop2.7"
from pyspark import SparkConf,SparkContext

sc = SparkSession.sparkContext #initializing spark with the python
#spark context initilazies the spark with the python

spark_session = SparkSession.builder.master("local").\
        appName("SparkApplication").\
        config("spark.driver.bindAddress","localhost").\
        config("spark.ui.port","4041").\
        getOrCreate()
sc = spark_session.sparkContext

spark_session

import pandas as pd

#Reading data as Pandas dataframe
pandas_df = pd.read_csv("C:\\Users\\Asus\\Downloads\\sales_data_sample.csv", encoding= 'unicode_escape')

pandas_df.head()

#checking the data types
pandas_df.dtypes

pandas_df.isnull().sum()



spark = SparkSession.builder.master("local").\
        appName("SparkApplication").\
        config("spark.driver.bindAddress","localhost").\
        config("spark.ui.port","4041").\
        getOrCreate()
sc = spark_session.sparkContext

spark_df = spark.read.csv("C:\\Users\\Asus\\Downloads\\sales_data_sample.csv",sep=',',inferSchema=True, header=True ,nullValue='')



#spark_df.head(50)

#spark_df

print(spark_df.printSchema())

print(spark_df.collect())

# df.dtypes
print(spark_df.dtypes)



#Checking for null values
from pyspark.sql.functions import col,isnan, when, count
spark_df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in spark_df.columns]).show()

#Replacing Null Values
spark_df = spark_df.na.fill({'ADDRESSLINE2': ''})
spark_df = spark_df.na.fill({'STATE': ''})
spark_df = spark_df.na.fill({'TERRITORY': ''})
spark_df = spark_df.na.fill({'POSTALCODE': ''})

spark_df.show()

#Checking for null values
from pyspark.sql.functions import col,isnan, when, count
spark_df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in spark_df.columns]).show()

#Checking weather Null vlues are replaced with NA
df=spark_df.select('ADDRESSLINE2', 'STATE','TERRITORY', 'POSTALCODE')

df.show()

#filtering country by USA
spark_df.filter(spark_df.COUNTRY == "USA").show(truncate=False)

#Broadcasting
spark = SparkSession.builder.appName('broadcasttt').getOrCreate()

states = {"NY":"New York", "CA":"California", "NJ":"New Jercy", "CT":"Connecticut", "MA":"Massachusetts", "PA":"Pennsylvania",
         "NSW":"New South Walse", "BC":"British Columbia", "NH":"New Hampshire"}

broadcastStates = spark.sparkContext.broadcast(states)

#unique oreder count
from pyspark.sql.functions import countDistinct

gr = spark_df.agg(countDistinct("ORDERNUMBER"))
gr.show()

#calculate delivery_cost column from (quantityordered*priceeach) - sales)
spark_df = spark_df.withColumn('DELIVARY_COST', (spark_df['QUANTITYORDERED']*spark_df['PRICEEACH']) - spark_df['SALES'])

spark_df.show()

#GroupBy country and avg of priceeach,quantityordered
spark_df = spark_df.groupBy("COUNTRY").agg({'PRICEEACH':'avg', 'QUANTITYORDERED':'avg'})

spark_df.show()

spark_df.write.option("header",True) \
        .mode("overwrite") \
        .csv("D:/ADATA/assignment_data")