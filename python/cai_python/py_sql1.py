#!/usr/bin/python
# -*- coding: UTF-8 -*-

import pyspark_init as pi

spark = pi.sql_init('sqltest1','local[1]')
df = spark.read.json('file:///D:/program/spark-2.3.2-bin-hadoop2.6/examples/src/main/resources/people.json')
df.show()
# 打印schema
df.cache()
df.printSchema()
# 选择多列
df.select(df.name,df.age+2).show()
# 条件过滤
df.filter(df.age>20).show()

# 写成parquet文件
# df.write.parquet('file:///E:/work_files/test_parq')

# 读取parquet文件
df2 = spark.read.parquet('file:///E:/work_files/test_parq')
print '*****  this is df2 parquetfile ****'
df2.show()