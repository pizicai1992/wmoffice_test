#!/usr/bin/python
# -*- coding: UTF-8 -*-

from optparse import OptionParser
import os
import datetime
from xml.dom.minidom import parse
import xml.dom.minidom
import subprocess

parser = OptionParser()
parser.add_option("-b", "--base", dest="base", help="oracle的库标识")
parser.add_option("-t", "--table", dest="table", help="oracle的表名")
(options, args) = parser.parse_args()
v_base = options.base
v_table = options.table

startTime = datetime.datetime.now()
hive_table = v_table.lower()+'_from_oracle'
oracle_table = v_table.upper()
java_file = oracle_table+'.java'
hdfs_dir='hdfs://CDH-cluster-main/user/hive/warehouse/bi_dw.db/'+hive_table

# 删除已有的表和HDFS目录
drop_hiveTable = 'hive -e "drop table if exists default.'+hive_table+';"'
rm_hdfsDir = 'hdfs dfs -rm -r -skipTrash '+hdfs_dir
sbp_drop = subprocess.Popen(drop_hiveTable, shell=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
sbp_drop.wait()
print sbp_drop.stdout.read()
sbp_rm = subprocess.Popen(rm_hdfsDir, shell=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
sbp_rm.wait()
print sbp_rm.stdout.read()

# 获得sqoop连接串
DomTree = xml.dom.minidom.parse('/scripts/dataware_house/program/config_file/ora_conf.xml')
collection = DomTree.documentElement
databases = collection.getElementsByTagName('database')
username = ''
password = ''
ip_port = ''
sid = ''
link = ''
def print_oracle_jdbc_sqoop(dbname):
    for database in databases:
        name = database.getElementsByTagName('name')[0].childNodes[0].data
        if name == dbname:
            username = database.getElementsByTagName('username')[0].childNodes[0].data
            password = database.getElementsByTagName('password')[0].childNodes[0].data
            ip_port = database.getElementsByTagName('ip_port')[0].childNodes[0].data
            sid = database.getElementsByTagName('sid')[0].childNodes[0].data
            link = 'jdbc:oracle:thin:@'+ip_port+'/'+sid+' -username '+username+' -password '+password
    return link
sqoop_link = print_oracle_jdbc_sqoop(v_base)

# 使用sqoop导入
sqoop_command="""
export HADOOP_OPTS=-Djava.security.egd=file:/dev/../dev/urandom
sqoop import -D mapreduce.map.memory.mb=24576 -D mapreduce.map.java.opts=-Xmx18432m \
-D mapred.job.name=sqoop_"""+hive_table+""" \
-D mapred.child.java.opts="-Djava.security.egd=file:/dev/../dev/urandom" \
--hive-import --hive-table """+hive_table+""" \
--connect """+sqoop_link+""" \
--table """+oracle_table+""" \
--target-dir """+hdfs_dir+""" \
--m 1 \
--null-string '\\\N' \
--null-non-string '\\\N' \
--hive-drop-import-delims 
"""
print sqoop_command
result = os.popen(sqoop_command)
print result.read()
endTime = datetime.datetime.now()
print 'oracle table is :', oracle_table
print 'hive table is : default.',hive_table
print 'java file is :',java_file
os.remove(java_file)
print 'sqoop task runtimes :', str((endTime-startTime).seconds), '秒'