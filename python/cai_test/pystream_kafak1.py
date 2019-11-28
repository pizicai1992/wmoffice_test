#!/usr/bin/python
# -*- coding: UTF-8 -*-
import sys
reload(sys)
sys.setdefaultencoding( "utf-8" )

from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils,TopicAndPartition

offsetRanges = []
def storeOffsetRanges(rdd):
    global offsetRanges
    offsetRanges = rdd.offsetRanges()
    return rdd
def printOffsetRanges(rdd):
    for o in offsetRanges: print "%s %s %s %s %s" % (
        o.topic, o.partition, o.fromOffset, o.untilOffset, o.untilOffset - o.fromOffset)


conf = SparkConf().setAppName('test_pyspark').setMaster('local[2]')
sc=SparkContext(conf = conf)
sc.setLogLevel('ERROR')
ssc=StreamingContext(sc,2)
brokers = "localhost:9092"
topic = "test_cai"
dstream = KafkaUtils.createDirectStream(ssc,[topic],kafkaParams={"metadata.broker.list":brokers})
wdcount = dstream.map(lambda x: x[1]).flatMap(lambda x:x.split(' ')).map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y)
wdcount.pprint()
dstream.transform(storeOffsetRanges).foreachRDD(printOffsetRanges)
ssc.start()
ssc.awaitTerminationOrTimeout(300)
ssc.stop()
