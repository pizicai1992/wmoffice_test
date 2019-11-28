#!/usr/bin/python
# -*- coding: UTF-8 -*-
'''
sqoop导出Oracle数据到hive表
'''
from xml.dom.minidom import parse
import xml.dom.minidom
import subprocess
import os
import datetime
import cx_Oracle as cx
from optparse import OptionParser

class getOraConn():
    '获取sqoop的JDBC连接串'
    jdbclink = ''
    oralink = ''
    def __init__(self,dbname):
        self.ora_conf = '/scripts/dataware_house/program/config_file/ora_conf.xml'
        self.domtree = xml.dom.minidom.parse(self.ora_conf)
        self.databases = self.domtree.documentElement.getElementsByTagName('database')
        self.dbname = dbname

    def jbdc_link(self):
        global jdbclink
        global oralink
        for database in self.databases:
            name = database.getElementsByTagName('name')[0].childNodes[0].data
            if name == self.dbname:
                username = database.getElementsByTagName('username')[0].childNodes[0].data
                password = database.getElementsByTagName('password')[0].childNodes[0].data
                ip_port = database.getElementsByTagName('ip_port')[0].childNodes[0].data
                sid = database.getElementsByTagName('sid')[0].childNodes[0].data
                jdbclink = 'jdbc:oracle:thin:@' + ip_port + '/' + sid + ' -username ' + username + ' -password ' + password
                oralink = username+'/'+password+'@'+ip_port+'/'+sid
        return jdbclink,oralink,username

class sqoopUtil():
    '删除hdfs中已有的目录和表'
    def __init__(self,basename,tabname,mapnum=1,*splitkey):
        self.conn = getOraConn(basename)
        self.sqooplink,self.oraclelink,self.orausername = self.conn.jbdc_link()
        print self.sqooplink
        self.mapnum = mapnum
        self.splitkey = splitkey
        self.hive_table = tabname.lower()+'_from_oracle'
        self.ora_table = tabname.upper()
        self.java_file = self.ora_table + '.java'
        self.hdfs_dir = 'hdfs://CDH-cluster-main/user/hive/warehouse/tmp.db/'+self.hive_table
    def dropdata(self):
        drop_hiveTable = 'hive -e -v "drop table if exists default.' + self.hive_table + ';"'
        rm_hdfsDir = 'hdfs dfs -rm -r -skipTrash ' + self.hdfs_dir
        sbp_drop = subprocess.Popen(drop_hiveTable, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        sbp_drop.wait()
        print sbp_drop.stdout.read()
        sbp_rm = subprocess.Popen(rm_hdfsDir, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        sbp_rm.wait()
        print sbp_rm.stdout.read()
    def get_hivecolumn(self):
        column = ()
        hive_column = ''
        conn = cx.connect(self.oraclelink)
        cursor = conn.cursor()
        cursor.prepare(
            "select upper(column_name)||'=string' from user_tab_columns where upper(table_name) = :tab_name order by column_id")
        cursor.execute(None, {'tab_name': self.ora_table})
        result = cursor.fetchall()
        for c in result:
            column = column + c
        cursor.close()
        conn.close()
        hive_column = ','.join(column)
        return hive_column
    def exec_sqoop(self):
        self.dropdata()
        hive_columns = self.get_hivecolumn()
        self.sqoop_command = """
        export HADOOP_OPTS=-Djava.security.egd=file:/dev/../dev/urandom
        sqoop import -D mapreduce.map.memory.mb=2048 -D mapreduce.map.java.opts=-Xmx1536m \
        -D mapred.job.name=sqoop_"""+self.hive_table+""" \
        -D mapred.child.java.opts="-Djava.security.egd=file:/dev/../dev/urandom" \
        --hive-import --hive-table """+self.hive_table+""" \
        --connect """+self.sqooplink+""" \
        --table """+str(self.orausername).upper()+"""."""+self.ora_table+""" \
        --target-dir """+self.hdfs_dir+""" \
        --null-string '\\\N' \
        --null-non-string '\\\N' \
        --hive-drop-import-delims --map-column-hive """+hive_columns
        if self.mapnum > 1:
            self.sqoop_command = self.sqoop_command+" --split-by "+self.splitkey+ \
                " --num-mappers "+self.mapnum
        else:
            self.sqoop_command = self.sqoop_command+" --m 1"
        print 'sqoop_command is :\n'+self.sqoop_command
        result = os.popen(self.sqoop_command)
        print result.read()
        print 'oracle table is :',self.ora_table
        print 'hive table is : tmp.'+self.hive_table
        print 'java file is :'+self.java_file
        if os.access(self.java_file, os.F_OK):
            os.remove(self.java_file)

if __name__ == '__main__':
    startTime = datetime.datetime.now()
    parser = OptionParser()
    parser.add_option("-b", "--base", dest="base", help="oracle的库标识")
    parser.add_option("-t", "--table", dest="table", help="oracle的表名")
    parser.add_option("-m", "--map", dest="map", help="指定map数量")
    parser.add_option("-s", "--split", dest="split", help="oracle表分割字段")
    (options, args) = parser.parse_args()
    v_base = options.base
    v_table = options.table
    v_map = options.map
    v_split = options.split
    execsqoop = sqoopUtil(v_base,v_table,v_map,v_split)
    execsqoop.exec_sqoop()
    endTime = datetime.datetime.now()
    print 'sqoop task runtimes :', str((endTime - startTime).seconds), '秒'
