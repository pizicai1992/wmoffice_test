#!/usr/bin/python
# -*- coding: UTF-8 -*-

import pyspark_init as pi
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

spark = pi.sql_init('testAPP', 'local[2]')
lines = spark.readStream.format('kafka').option('kafka.bootstrap.servers', '192.168.10.10:9092').option()