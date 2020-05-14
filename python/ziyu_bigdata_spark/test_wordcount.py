# -*- coding: UTF-8 -*-
from pyspark import SparkContext

def printx(somestr):
    print somestr

# 加载环境
sc = SparkContext("local[2]","test_wordcount")
# 加载数据
textfile = sc.textFile('E:/work_files/updte_hiveodi_status.log').flatMap(lambda line:line.split(" "))
wordcount = textfile.map(lambda line:(line, 1)).reduceByKey(lambda a, b:a + b)
wordcount.foreach(lambda x:printx(x))