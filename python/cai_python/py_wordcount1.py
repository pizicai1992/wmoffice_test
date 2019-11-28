#!/usr/bin/python
# -*- coding: UTF-8 -*-

import pyspark_init as pi
import re


def printx(x):
    print x


sc = pi.spark_init('test1', 'local[2]')
# sc.setLogLevel('INFO')
testfile = sc.textFile('file:///E:/work_files/2018091401.txt')
testfile.cache()
file_count = testfile.count()
print 'file count is : %d' % file_count
file_first = testfile.first()
print 'file firstline is : %s' % file_first

wd = testfile.flatMap(lambda x: re.split('\s', str(x).strip())).map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y). \
    filter(lambda x: len(x[0]) > 0).map(lambda x: (x[1], x[0])).sortByKey(ascending=False).map(lambda x: (x[1], x[0])) \
    .filter(lambda x: x[1] > 20)
wd.saveAsTextFile('file:///E:/work_files/2018091401_rsult.txt')
wd_result = wd.collect()
# print wd_result
for w, d in wd_result:
    print 'word is :%s \tcount is :%d' % (w, d)

wd.mapValues(lambda x: x + 2).foreach(printx)  # 对键值对RDD中的每个value都应用一个函数，但是，key不会发生变化
sc.stop()
