# -*- coding: utf-8 -*-
"""
Created on Tue Sep 13 10:36:27 2022

@author: clivinjohn.geju
"""

import findspark
findspark.init()

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
columns = ["Seqno","Quote"]
data = [("1", "Be the change that you wish to see in the world"),
    ("2", "Everyone thinks of changing the world, but no one thinks of changing himself."),
    ("3", "The purpose of our lives is to be happy."),
    ("4", "Be cool.")]
df = spark.createDataFrame(data,columns)
df.show()

df.show(truncate=False)

df.show(2,truncate=False)

df.show(2,truncate=25) 

df.show(n=3,truncate=25,vertical=True)