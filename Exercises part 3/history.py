# -*- coding: utf-8 -*-
# *** Spyder Python Console History Log ***

## ---(Mon Sep 12 11:09:53 2022)---
runfile('C:/Users/clivinjohn.geju/.spyder-py3/temp.py', wdir='C:/Users/clivinjohn.geju/.spyder-py3')
pip install pyspark
runfile('C:/Users/clivinjohn.geju/.spyder-py3/temp.py', wdir='C:/Users/clivinjohn.geju/.spyder-py3')
EXIT
exit()
runfile('C:/Users/clivinjohn.geju/.spyder-py3/temp.py', wdir='C:/Users/clivinjohn.geju/.spyder-py3')
import findspark
findspark.init()
from pyspark/sql import SparkSession
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[1]").appName("SparkByExamples.com").getOrCreate()
dataList = [("Java", 20000), ("Python", 100000), ("Scala", 3000)]
rdd = spark.sparkContext.parallelize(dataList)
rdd2 = spark.sparkContext.textFile("/path/text/txt")
('Jen', 'Mary', 'Brown', '1980-02-17', 'F', -1)]
]
data = [('James', '', 'Smith', '1991-04-01', 'M', 3000),
data = [('James', '', 'Smith', '1991-04-01', 'M', 3000),
        ('Micheal', 'Rose', '', '2000-05-19', 'M', 4000),
        ('Robert', '', 'Williams', '1978-09-05', 'M', 4000),
        ('Maria', 'Anne', 'Jones', '1967-12-01', 'F', 4000),
        ('Jen', 'Mary', 'Brown', '1980-02-17', 'F', -1)
        ]
runcell(0, 'C:/Users/clivinjohn.geju/.spyder-py3/untitled0.py')
exit()
import findspark
findspark.init()
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[1]").appName("SparkByExamples.com").getOrCreate()
dataList = [("Java", 20000), ("Python", 100000), ("Scala", 3000)]
rdd = spark.sparkContext.parallelize(dataList)
rdd2 = spark.sparkContext.textFile("/path/text/txt")
exit()
import findspark
findspark.init()
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[1]").appName("SparkByExamples.com").getOrCreate()
dataList = [("Java", 20000), ("Python", 100000), ("Scala", 3000)]
rdd = spark.sparkContext.parallelize(dataList)
rdd2 = spark.sparkContext.textFile("/path/text.txt")
data = [('James', '', 'Smith', '1991-04-01', 'M', 3000),
        ('Micheal', 'Rose', '', '2000-05-19', 'M', 4000),
        ('Robert', '', 'Williams', '1978-09-05', 'M', 4000),
        ('Maria', 'Anne', 'Jones', '1967-12-01', 'F', 4000),
        ('Jen', 'Mary', 'Brown', '1980-02-17', 'F', -1)
        ]
runcell(0, 'C:/Users/clivinjohn.geju/.spyder-py3/untitled0.py')
columns = ["Firstname", "Middlename", "lastname", "dob", "gender", "salary"]
df = spark.createDataFrame(data=data, schema=columns)
df.show()

## ---(Mon Sep 12 15:27:30 2022)---
import findspark
findspark.init()
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[1]").appName("SparkByExamples.com").getOrCreate()
dataList = [("Java", 20000), ("Python", 100000), ("Scala", 3000)]
rdd = spark.sparkContext.parallelize(dataList)
rdd2 = spark.sparkContext.textFile("/path/text.txt")
data = [('James', '', 'Smith', '1991-04-01', 'M', 3000),
        ('Micheal', 'Rose', '', '2000-05-19', 'M', 4000),
        ('Robert', '', 'Williams', '1978-09-05', 'M', 4000),
        ('Maria', 'Anne', 'Jones', '1967-12-01', 'F', 4000),
        ('Jen', 'Mary', 'Brown', '1980-02-17', 'F', -1)
        ]
columns = ["Firstname", "Middlename", "lastname", "dob", "gender", "salary"]
df = spark.createDataFrame(data=data, schema=columns)
df.show()
df.createOrReplaceTempView("PERSON_DATA")
df2 = spark.sql("SELECT * from PERSON_DATA")
df2.printSchema()
df2.show()
groupDF = spark.sql("SELECT gender, count(*) from PERSON_DATA group by gender")
groupDF.show()
df = spark.readStream.format("socket").option("host", "localhost").option("port", "9090").load()
exit()
spark = SparkSession.builder.appName('SparkbyExamples.com').getOrCreate()
exit()

## ---(Tue Sep 13 09:58:12 2022)---
runfile('C:/Users/clivinjohn.geju/.spyder-py3/spint.py', wdir='C:/Users/clivinjohn.geju/.spyder-py3')
runfile('C:/Users/clivinjohn.geju/.spyder-py3/untitled1.py', wdir='C:/Users/clivinjohn.geju/.spyder-py3')
runfile('C:/Users/clivinjohn.geju/.spyder-py3/untitled2.py', wdir='C:/Users/clivinjohn.geju/.spyder-py3')
runfile('C:/Users/clivinjohn.geju/.spyder-py3/untitled3.py', wdir='C:/Users/clivinjohn.geju/.spyder-py3')
runfile('C:/Users/clivinjohn.geju/.spyder-py3/untitled4.py', wdir='C:/Users/clivinjohn.geju/.spyder-py3')
runfile('C:/Users/clivinjohn.geju/.spyder-py3/untitled5.py', wdir='C:/Users/clivinjohn.geju/.spyder-py3')
runfile('C:/Users/clivinjohn.geju/.spyder-py3/untitled6.py', wdir='C:/Users/clivinjohn.geju/.spyder-py3')
runfile('C:/Users/clivinjohn.geju/.spyder-py3/untitled7.py', wdir='C:/Users/clivinjohn.geju/.spyder-py3')
runfile('C:/Users/clivinjohn.geju/.spyder-py3/untitled8.py', wdir='C:/Users/clivinjohn.geju/.spyder-py3')
runfile('C:/Users/clivinjohn.geju/.spyder-py3/untitled9.py', wdir='C:/Users/clivinjohn.geju/.spyder-py3')
runfile('C:/Users/clivinjohn.geju/.spyder-py3/untitled10.py', wdir='C:/Users/clivinjohn.geju/.spyder-py3')
runfile('C:/Users/clivinjohn.geju/.spyder-py3/untitled11.py', wdir='C:/Users/clivinjohn.geju/.spyder-py3')
runfile('C:/Users/clivinjohn.geju/.spyder-py3/untitled12.py', wdir='C:/Users/clivinjohn.geju/.spyder-py3')
runfile('C:/Users/clivinjohn.geju/.spyder-py3/untitled13.py', wdir='C:/Users/clivinjohn.geju/.spyder-py3')
runfile('C:/Users/clivinjohn.geju/.spyder-py3/untitled14.py', wdir='C:/Users/clivinjohn.geju/.spyder-py3')
runfile('C:/Users/clivinjohn.geju/.spyder-py3/untitled15.py', wdir='C:/Users/clivinjohn.geju/.spyder-py3')
runfile('C:/Users/clivinjohn.geju/.spyder-py3/untitled16.py', wdir='C:/Users/clivinjohn.geju/.spyder-py3')
runfile('C:/Users/clivinjohn.geju/.spyder-py3/untitled17.py', wdir='C:/Users/clivinjohn.geju/.spyder-py3')
runfile('C:/Users/clivinjohn.geju/.spyder-py3/untitled18.py', wdir='C:/Users/clivinjohn.geju/.spyder-py3')
runfile('C:/Users/clivinjohn.geju/.spyder-py3/untitled19.py', wdir='C:/Users/clivinjohn.geju/.spyder-py3')
runfile('C:/Users/clivinjohn.geju/.spyder-py3/untitled20.py', wdir='C:/Users/clivinjohn.geju/.spyder-py3')
runfile('C:/Users/clivinjohn.geju/.spyder-py3/untitled22.py', wdir='C:/Users/clivinjohn.geju/.spyder-py3')
runfile('C:/Users/clivinjohn.geju/.spyder-py3/untitled23.py', wdir='C:/Users/clivinjohn.geju/.spyder-py3')
runfile('C:/Users/clivinjohn.geju/.spyder-py3/untitled24.py', wdir='C:/Users/clivinjohn.geju/.spyder-py3')
runfile('C:/Users/clivinjohn.geju/.spyder-py3/untitled26.py', wdir='C:/Users/clivinjohn.geju/.spyder-py3')
runfile('C:/Users/clivinjohn.geju/.spyder-py3/untitled27.py', wdir='C:/Users/clivinjohn.geju/.spyder-py3')
runfile('C:/Users/clivinjohn.geju/.spyder-py3/untitled28.py', wdir='C:/Users/clivinjohn.geju/.spyder-py3')
runfile('C:/Users/clivinjohn.geju/.spyder-py3/untitled29.py', wdir='C:/Users/clivinjohn.geju/.spyder-py3')