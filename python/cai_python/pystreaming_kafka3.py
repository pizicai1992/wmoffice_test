#!/usr/bin/python
# -*- coding: UTF-8 -*-

import pyspark_init as pi
from pyspark.streaming.kafka import KafkaUtils
import cx_Oracle as cx


is_sql = "insert into t_test_cai20181213 VALUES (:1,:2)"

def ora_insert(dstream_data):
    dstream_data1=dstream_data.map(lambda x:list(x))
    paras=[]
    for i in dstream_data1: #.collect():
        para = (str(i[0]), int(i[1]))
        paras.append(para)

    print paras
    db_con = cx.connect('hive/hive_2017@10.14.250.51:1521/dfdb')
    cursor = db_con.cursor()

    try:
        cursor.executemany(is_sql,paras)
        db_con.commit()
    except Exception, e:
        print e
        db_con.rollback()
    finally:
        cursor.close()
        db_con.close()

def rddfun(rdd):
    ddata = rdd.repartition(1)
    ddata.foreachPartition(ora_insert)


ssc = pi.streaming_init('streaming_kafka1','local[2]',3)
brokers = "localhost:9092"
topic = "testcai_1"
zk = "localhost:2181"

kvs = KafkaUtils.createStream(ssc,zkQuorum=zk,topics={topic: 1},\
                              kafkaParams={"metadata.broker.list":brokers},groupId='test1')
#注意 取tuple下的第二个即为接收到的kafka流
lines = kvs.map(lambda x:x[1]).map(lambda x:x.split(' ')).reduceByKey(lambda x,y:x+y)
lines.pprint()
lines.foreachRDD(rddfun)
ssc.start()
ssc.awaitTerminationOrTimeout(30)
ssc.stop()