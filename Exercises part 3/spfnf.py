# -*- coding: utf-8 -*-
"""
Created on Tue Sep 13 15:42:41 2022

@author: clivinjohn.geju
"""

import findspark
findspark.init()

from pyspark.sql import SparkSession
spark=SparkSession.builder.master("local[1]").appName("sparkbyexamples.com").getOrCreate()


from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .master("local[1]") \
    .appName("SparkByExamples.com") \
    .getOrCreate()

filePath="resources/small_zipcode.csv"
df = spark.read.options(header='true', inferSchema='true') \
          .csv(filePath)

df.printSchema()
df.show(truncate=False)

#Replace 0 for null for all integer columns
df.na.fill(value=0).show()

#Replace 0 for null on only population column 
df.na.fill(value=0,subset=["population"]).show()

df.na.fill("").show(False)

df.na.fill({"city": "unknown", "type": ""}) \
    .show()
