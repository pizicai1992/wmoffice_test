#!/usr/bin/python
# -*- coding: UTF-8 -*-

import pyspark_init as pi
import json
# import import_testfile as conftest

sc = pi.spark_init('test3', 'local[2]')
# read json file
jsfile = sc.textFile('file:///D:/program/spark-2.3.2-bin-hadoop2.6/examples/src/main/resources/people.json')
jsfile.foreach(lambda x:pi.printx(x))
# json 解析
result = jsfile.map(lambda x:json.loads(x))
result.foreach(lambda x:pi.printx(x))

# print conftest.conf