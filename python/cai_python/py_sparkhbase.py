#!/usr/bin/python
# -*- coding: UTF-8 -*-

import pyspark_init as pi

sc = pi.spark_init('test4', 'local[2]')
host = 'cdhdev1'
table = 't_test_hive2hbase'
conf = {"hbase.zookeeper.quorum": host, "hbase.mapreduce.inputtable": table}
keyConv = "org.apache.spark.examples.pythonconverters.ImmutableBytesWritableToStringConverter"
valueConv = "org.apache.spark.examples.pythonconverters.HBaseResultToStringConverter"
hbase_rdd = sc.newAPIHadoopRDD("org.apache.hadoop.hbase.mapreduce.TableInputFormat",\
                               "org.apache.hadoop.hbase.io.ImmutableBytesWritable",\
                               "org.apache.hadoop.hbase.client.Result",\
                               keyConverter=keyConv,\
                               valueConverter=valueConv,conf=conf)
count = hbase_rdd.count()
hbase_rdd.cache()
out_hbase = hbase_rdd.collect()
for (k,v) in out_hbase:
    print (k,v)

print 'table count is : '+str(count)
sc.stop()