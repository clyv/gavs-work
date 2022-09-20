# -*- coding: utf-8 -*-
"""
Created on Tue Sep 13 15:19:45 2022

@author: clivinjohn.geju
"""

import findspark
findspark.init()

#Create DataFrame df1 with columns name,dept & age
val data = Seq(("James","Sales",34), ("Michael","Sales",56), 
               ("Robert","Sales",30), ("Maria","Finance",24) )
import spark.implicits._
val df1 = data.toDF("name","dept","age")
df1.printSchema()

#Create DataFrame df1 with columns name,dep,state & salary
val data2=Seq(("James","Sales","NY",9000),("Maria","Finance","CA",9000),
              ("Jen","Finance","NY",7900),("Jeff","Marketing","CA",8000))
val df2 = data2.toDF("name","dept","state","salary")
df2.printSchema()

val merged_cols = df1.columns.toSet ++ df2.columns.toSet
import org.apache.spark.sql.functions.{col,lit}
def getNewColumns(column: Set[String], merged_cols: Set[String]) = {
    merged_cols.toList.map(x => x match {
      case x if column.contains(x) => col(x)
      case _ => lit(null).as(x)
    })
}
val new_df1=df1.select(getNewColumns(df1.columns.toSet, merged_cols):_*)
val new_df2=df2.select(getNewColumns(df2.columns.toSet, merged_cols):_*)

val merged_df=new_df1.unionByName(new_df2)
merged_df.show()

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

#Create DataFrame df1 with columns name,dept & age
data = [("James","Sales",34), ("Michael","Sales",56), \
    ("Robert","Sales",30), ("Maria","Finance",24) ]
columns= ["name","dept","age"]
df1 = spark.createDataFrame(data = data, schema = columns)
df1.printSchema()

#Create DataFrame df1 with columns name,dep,state & salary
data2=[("James","Sales","NY",9000),("Maria","Finance","CA",9000), \
    ("Jen","Finance","NY",7900),("Jeff","Marketing","CA",8000)]
columns2= ["name","dept","state","salary"]
df2 = spark.createDataFrame(data = data2, schema = columns2)
df2.printSchema()

#Add missing columns 'state' & 'salary' to df1
from pyspark.sql.functions import lit
for column in [column for column in df2.columns if column not in df1.columns]:
    df1 = df1.withColumn(column, lit(None))

#Add missing column 'age' to df2
for column in [column for column in df1.columns if column not in df2.columns]:
    df2 = df2.withColumn(column, lit(None))

#Finally join two dataframe's df1 & df2 by name
merged_df=df1.unionByName(df2)
merged_df.show()
