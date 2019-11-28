# -*- coding: UTF-8 -*-
import sys
reload(sys)
sys.setdefaultencoding("utf-8")

from pyspark import SparkConf,SparkContext
#from __future__ import print_function

sc = SparkContext("local[2]", "test1")
def pf(x):
    print x

tfile = sc.textFile('E:\\UE_file\\1111').filter(lambda x:len(x)>0).flatMap(lambda x:x.split(' ')).filter(lambda x:len(x)>0)
word_count = tfile.map(lambda x:(x,1)).reduceByKey(lambda x,y: x+y)
word_count.foreach(pf)
# word_count.foreach(print)
#for wc in word_count.take(30):
#    print wc
# 使用take 后将rdd变成了list类型
