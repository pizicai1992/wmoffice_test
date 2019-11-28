#!/usr/bin/python
# -*- coding: UTF-8 -*-

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext

def spark_init(appname,master):
    conf = SparkConf().setAppName(appname).setMaster(master)
    sc = SparkContext(conf=conf)
   # sc.setLogLevel('ERROR')
    return sc

def printx(x):
    print x

def sql_init(appname,master):
    spark=SparkSession.builder.appName(appname).master(master).getOrCreate()
    return spark

def streaming_init(appname,master,time):
    conf = SparkConf().setAppName(appname).setMaster(master)
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, time)
    return ssc