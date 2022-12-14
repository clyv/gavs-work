# -*- coding: utf-8 -*-
"""
Created on Tue Sep 13 10:54:01 2022

@author: clivinjohn.geju
"""

import findspark
findspark.init()

from pyspark.sql import Row
row=Row("James",40)
print(row[0] +","+str(row[1]))

row=Row(name="Alice", age=11)
print(row.name) 

Person = Row("name", "age")
p1=Person("James", 40)
p2=Person("Alice", 35)
print(p1.name +","+p2.name)

from pyspark.sql import SparkSession, Row
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

data = [Row(name="James,,Smith",lang=["Java","Scala","C++"],state="CA"), 
    Row(name="Michael,Rose,",lang=["Spark","Java","C++"],state="NJ"),
    Row(name="Robert,,Williams",lang=["CSharp","VB"],state="NV")]
rdd=spark.sparkContext.parallelize(data)
print(rdd.collect())

[Row(name='James,,Smith', lang=['Java', 'Scala', 'C++'], state='CA'), Row(name='Michael,Rose,', lang=['Spark', 'Java', 'C++'], state='NJ'), Row(name='Robert,,Williams', lang=['CSharp', 'VB'], state='NV')]

collData=rdd.collect()
for row in collData:
    print(row.name + "," +str(row.lang))

Person=Row("name","lang","state")
data = [Person("James,,Smith",["Java","Scala","C++"],"CA"), 
    Person("Michael,Rose,",["Spark","Java","C++"],"NJ"),
    Person("Robert,,Williams",["CSharp","VB"],"NV")]

df=spark.createDataFrame(data)
df.printSchema()
df.show()

columns = ["name","languagesAtSchool","currentState"]
df=spark.createDataFrame(data).toDF(*columns)
df.printSchema()

#Create DataFrame with struct using Row class
from pyspark.sql import Row
data=[Row(name="James",prop=Row(hair="black",eye="blue")),
      Row(name="Ann",prop=Row(hair="grey",eye="black"))]
df=spark.createDataFrame(data)
df.printSchema()
