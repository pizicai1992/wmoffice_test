#!/usr/bin/python
# -*- coding: UTF-8 -*-
import sys
reload(sys)
sys.setdefaultencoding( "utf-8" )
from pyspark import SparkContext,SparkConf
from pyspark.streaming import StreamingContext
from operator import add
#conf = SparkConf().setAppName('test_pyspark')
#sc=SparkContext(conf = conf)
sc=SparkContext("local[2]","pytest")
sc.setLogLevel('ERROR')
ssc=StreamingContext(sc, 2)
lines = ssc.socketTextStream("localhost",9999)
words = lines.flatMap(lambda x:x.split('\\s+'))
wdcounts = words.map(lambda x:(x,1)).reduceByKey(add).filter(lambda (x,y):y>0)
wdcounts.pprint()
#wdcounts.saveAsTextFiles('cat_test/test_file')
ssc.start()
ssc.awaitTermination(120)
