#!/usr/bin/python
# -*- coding: UTF-8 -*-

import pyspark_init as pi

sc = pi.spark_init('test2', 'local[2]')
prdd1 = sc.parallelize([('spark',1),('spark',2),('hadoop',3),('hadoop',5)])
prdd2 = sc.parallelize([('spark','fast')])
prdd3 = prdd1.join(prdd2)
prdd3.foreach(lambda x:pi.printx(x))
# 键值对的key表示图书名称，value表示某天图书销量，请计算每个键对应的平均值，也就是计算每种图书的每天平均销量
prdd4 = sc.parallelize([("spark",2),("hadoop",6),("hadoop",4),("spark",6)])
prdd4.mapValues(lambda x:(x,1)).reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1])).mapValues(lambda x:x[0]/x[1])\
    .foreach(lambda x:pi.printx(x))

# reduceByKey(func)
prdd1.reduceByKey(lambda x,y:x+y).foreach(lambda x:pi.printx('reduceByKey: \t'+str(x)))

# groupByKey()对具有相同键的值进行分组。比如，对四个键值对(“spark”,1)、(“spark”,2)、(“hadoop”,3)和(“hadoop”,5)，
# 采用groupByKey()后得到的结果是：(“spark”,(1,2))和(“hadoop”,(3,5))
prdd1.groupByKey().foreach(lambda x:pi.printx(x))
# keys()
prdd1.groupByKey().keys().foreach(lambda x:pi.printx('key is :\t'+str(x)))
# values()
prdd1.groupByKey().values().foreach(lambda x:pi.printx(x))

# sortByKey() 功能是返回一个根据键排序的RDD
prdd1.sortByKey().foreach(lambda x:pi.printx(x))
