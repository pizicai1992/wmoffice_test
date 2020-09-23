#!/usr/bin/python
# -*- coding: UTF-8 -*-

import pyspark_init as pi
import json
import sys
import os
# import import_testfile as conftest

print sys.path
BASE_PATH = 'd:/wmoffice_test/python/cai_python'
sys.path.append(BASE_PATH)
print BASE_PATH

sc = pi.spark_init('test3', 'local[2]')
# read json file
jsfile = sc.textFile('file:///C:/mysoft/BigData/spark/examples/src/main/resources/people.json')
jsfile.foreach(lambda x:pi.printx(x))
# json 解析
result = jsfile.map(lambda x:json.loads(x))
result.foreach(lambda x:pi.printx(x))

# print conftest.conf