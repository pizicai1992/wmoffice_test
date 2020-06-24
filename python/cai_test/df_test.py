# -*- coding: UTF-8 -*-

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import Row

appname = 'dataframe_test'
spark=SparkSession.builder.appName(appname).getOrCreate()
# conf = SparkConf().setAppName(appname)
# sc = SparkContext(conf=conf)
sc = spark.sparkContext
sc.setLogLevel("WARN")

# hdpFile = sc.wholeTextFiles('/export/gamelog/2020-06-19/sdxl.format.log/15/*')
hdpFile = sc.textFile('/export/gamelog/2020-06-19/sdxl.format.log/15/*')
hdpFile.take(10)
f_rdd1 = hdpFile.filter(lambda x: 'produce_get:' in x) \
                .map(lambda x: 'logtime=' + x.split(' produce_get:')[0].split('#')[-1] + \
                     '|' + x.split(' produce_get:')[1].replace(':', '|'))
print f_rdd1.take(10)
f_rdd2 = f_rdd1.map(lambda x: x.split('|'))
f_rdd3 = f_rdd2.map(lambda x:Row(**dict(zip([i.split('=')[0] for i in x],[j.split('=')[1] for j in x])))) 
df = spark.createDataFrame(f_rdd3)
df.show()