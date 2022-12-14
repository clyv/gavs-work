# -*- coding: utf-8 -*-
"""
Created on Tue Sep 13 15:34:52 2022

@author: clivinjohn.geju
"""

import findspark
findspark.init()

from pyspark.sql import SparkSession
spark=SparkSession.builder.master("local[1]").appName("sparkbyexamples.com").getOrCreate()


from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

data = [('James','Smith','M',30),('Anna','Rose','F',41),
  ('Robert','Williams','M',62), 
]
columns = ["firstname","lastname","gender","salary"]
df = spark.createDataFrame(data=data, schema = columns)
df.show()

from pyspark.sql.functions import concat_ws,col,lit
df.select(concat_ws(",",df.firstname,df.lastname).alias("name"), \
          df.gender,lit(df.salary*2).alias("new_salary")).show()

# Refering columns by index.
rdd=df.rdd.map(lambda x: 
    (x[0]+","+x[1],x[2],x[3]*2)
    )  
df2=rdd.toDF(["name","gender","new_salary"])
df2.show()

# Referring Column Names
rdd2=df.rdd.map(lambda x: 
    (x["firstname"]+","+x["lastname"],x["gender"],x["salary"]*2)
    ) 

# Referring Column Names
rdd2=df.rdd.map(lambda x: 
    (x.firstname+","+x.lastname,x.gender,x.salary*2)
    ) 

# By Calling function
def func1(x):
    firstName=x.firstname
    lastName=x.lastName
    name=firstName+","+lastName
    gender=x.gender.lower()
    salary=x.salary*2
    return (name,gender,salary)

rdd2=df.rdd.map(lambda x: func1(x))

# Foreach example
def f(x): print(x)
df.foreach(f)

# Another example
df.foreach(lambda x: 
    print("Data ==>"+x["firstname"]+","+x["lastname"]+","+x["gender"]+","+str(x["salary"]*2))
    ) 

# Using pandas
import pandas as pd
spark.conf.set("spark.sql.execution.arrow.enabled", "true")
pandasDF = df.toPandas()
for index, row in pandasDF.iterrows():
    print(row['firstname'], row['gender'])

# Collect the data to Python List
dataCollect = df.collect()
for row in dataCollect:
    print(row['firstname'] + "," +row['lastname'])

#Using toLocalIterator()
dataCollect=df.rdd.toLocalIterator()
for row in dataCollect:
    print(row['firstname'] + "," +row['lastname'])
