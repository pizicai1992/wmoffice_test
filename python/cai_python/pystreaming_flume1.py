#!/usr/bin/python
# -*- coding: UTF-8 -*-

import pyspark_init as pi
from pyspark.streaming.flume import FlumeUtils
import pyspark

ssc = pi.streaming_init('streaming_flume1','local[2]',3)
host = 'localhost'
port = 44444
dsm = FlumeUtils.createStream(ssc,host,port,pyspark.StorageLevel.MEMORY_AND_DISK_SER_2)
dsm.count().map(lambda x:'Recieve ' + str(x) + ' Flume events!!!!').pprint()
ssc.start()
ssc.awaitTerminationOrTimeout(120)
ssc.stop()