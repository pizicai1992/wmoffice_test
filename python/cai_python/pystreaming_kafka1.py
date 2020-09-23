#!/usr/bin/python
# -*- coding: UTF-8 -*-

import pyspark_init as pi
from pyspark.streaming.kafka import KafkaUtils

ssc = pi.streaming_init('streaming_kafka1','local[2]',3)
brokers = "192.168.10.10:9092"
topic = "testcai"
zk = "192.168.10.10:2181"

kvs = KafkaUtils.createStream(ssc,zkQuorum=zk,topics={topic: 1},\
                              kafkaParams={"metadata.broker.list":brokers},groupId='test1')
#注意 取tuple下的第二个即为接收到的kafka流
lines = kvs.map(lambda x:x[1]).map(lambda x:x.split(' ')).reduceByKey(lambda x,y:x+y)
lines.pprint()
ssc.start()
ssc.awaitTerminationOrTimeout(30)
ssc.stop()