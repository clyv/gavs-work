# -*- coding: utf-8 -*-
"""
Created on Tue Sep 13 15:40:03 2022

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

df=spark.range(100)
print(df.sample(0.06).collect())
#Output: [Row(id=0), Row(id=2), Row(id=17), Row(id=25), Row(id=26), Row(id=44), Row(id=80)]

print(df.sample(0.1,123).collect())
#Output: 36,37,41,43,56,66,69,75,83

print(df.sample(0.1,123).collect())
#Output: 36,37,41,43,56,66,69,75,83

print(df.sample(0.1,456).collect())
#Output: 19,21,42,48,49,50,75,80

print(df.sample(True,0.3,123).collect()) #with Duplicates
#Output: 0,5,9,11,14,14,16,17,21,29,33,41,42,52,52,54,58,65,65,71,76,79,85,96
print(df.sample(0.3,123).collect()) # No duplicates
#Output: 0,4,17,19,24,25,26,36,37,41,43,44,53,56,66,68,69,70,71,75,76,78,83,84,88,94,96,97,98

df2=df.select((df.id % 3).alias("key"))
print(df2.sampleBy("key", {0: 0.1, 1: 0.2},0).collect())
#Output: [Row(key=0), Row(key=1), Row(key=1), Row(key=1), Row(key=0), Row(key=1), Row(key=1), Row(key=0), Row(key=1), Row(key=1), Row(key=1)]

rdd = spark.sparkContext.range(0,100)
print(rdd.sample(False,0.1,0).collect())
#Output: [24, 29, 41, 64, 86]
print(rdd.sample(True,0.3,123).collect())
#Output: [0, 11, 13, 14, 16, 18, 21, 23, 27, 31, 32, 32, 48, 49, 49, 53, 54, 72, 74, 77, 77, 83, 88, 91, 93, 98, 99]

print(rdd.takeSample(False,10,0))
#Output: [58, 1, 96, 74, 29, 24, 32, 37, 94, 91]
print(rdd.takeSample(True,30,123))
#Output: [43, 65, 39, 18, 84, 86, 25, 13, 40, 21, 79, 63, 7, 32, 26, 71, 23, 61, 83, 60, 22, 35, 84, 22, 0, 88, 16, 40, 65, 84]
