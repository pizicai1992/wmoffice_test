#!/usr/bin/python
# -*- coding: UTF-8 -*-

import cx_Oracle as cx
import ConfigParser
import os

import time

import threadpool


class get_connect():
    '获得sqoop和cx_Oracle的连接串以及Oracle的用户名'

    def __init__(self, ora_db):
        config = ConfigParser.RawConfigParser()
        self.conf_file = 'E:\datawarehouse\datawarehouse\cfg\oracle_connect.config'
        config.read(self.conf_file)
        self.ora_username = str(config.get(ora_db, 'username'))
        self.ora_password = str(config.get(ora_db, 'password'))
        self.ora_ip = str(config.get(ora_db, 'ip'))
        self.ora_host = str(config.get(ora_db, 'host'))
        self.ora_servname = str(config.get(ora_db, 'service_name'))

    def jdbc_conn(self):
        jdbc_link = 'jdbc:oracle:thin:@%s:%s/%s -username %s -password %s' % \
                    (self.ora_ip, self.ora_host, self.ora_servname, self.ora_username, self.ora_password)
        print jdbc_link
        return jdbc_link

    def cx_ora_conn(self):
        cx_oar_link = '%s/%s@%s:%s/%s' % \
                      (self.ora_username, self.ora_password, self.ora_ip, self.ora_host, self.ora_servname)
        print cx_oar_link
        return cx_oar_link

    def ora_users(self):
        ora_user = self.ora_username
        return ora_user


# gtc = get_connect('dt_sdb')
# print gtc.jdbc_conn()

sqoop_cmd_list = []


def print_sqoop_cmd(ora_dbname, oracle_tab, hive_db, hive_tab):
    jar_dir = '/mnt/data0/logs/dataware_house/log/'
    get_conn = get_connect(ora_dbname)
    ora_conct = get_conn.cx_ora_conn()
    jdbc_connect = get_conn.jdbc_conn()
    ora_username = get_conn.ora_users()
    hive_table = hive_db + '.' + hive_tab
    sqoop_cmd = ''
    column = ()
    sql_str = """
        select column_name || '=' || decode(data_type,
                                        'NUMBER',
                                        'decimal',
                                        'VARCHAR2',
                                        'string',
                                        'DATE',
                                        'string',
                                        'string')
      from user_tab_columns
     where lower(table_name) = :tab_name
     order by column_id
    """
#    print ora_conct
    conn1 = cx.connect(ora_conct)
    cursor1 = conn1.cursor()
    cursor1.prepare(sql_str)
    cursor1.execute(None, {'tab_name': str(oracle_tab).lower()})
    result1 = cursor1.fetchall()
    for resu in result1:
        column = column + resu

    cursor1.close()
    conn1.close()
    hive_columns = ','.join(column)
    sqoop_cmd = """sqoop import -D mapreduce.map.memory.mb=2048 -D mapreduce.map.java.opts=-Xmx1536m \
 -D mapred.job.name=sqoop_""" + oracle_tab + """ \
 -D mapred.child.java.opts="-Djava.security.egd=file:/dev/../dev/urandom" \
 --hive-import --hive-table """ + hive_table + """ \
 --connect """ + jdbc_connect + """ \
 --table """ + str(ora_username).upper() + "." + str(oracle_tab).upper() + """ \
 --hive-overwrite """ + """ \
 --null-string '\\\N' \
 --null-non-string '\\\N' --outdir """ + jar_dir + """ \
 --hive-drop-import-delims --map-column-hive """ + hive_columns + " --m 1"
    return sqoop_cmd

def exec_cmd(cmd_str):
    print 'sqoop_cmd is :'+'\n'+cmd_str
    result = os.popen(cmd_str)
    print result.read()

conn = cx.connect('testuser/testuser@192.168.10.10:1521/wmdw')
cursor = conn.cursor()
cursor.execute("select * from t_dic_sqoop_testcai")
result = cursor.fetchall()
cursor.close()
conn.close()
for res in result:
    print res[0], res[1], res[2], res[3]
    sqlss = print_sqoop_cmd(res[0],res[1],res[2],res[3])
    sqoop_cmd_list.append(sqlss)
    print sqlss
    print
#print print_sqoop_cmd('new_db', 't_dw_zx_dic_serverlist', 'bi_tmp', 't_dim_zx_dic_serverlist')
print sqoop_cmd_list
start_time = time.time()
pool = threadpool.ThreadPool(3)
requests = threadpool.makeRequests(exec_cmd, sqoop_cmd_list)
[pool.putRequest(req) for req in requests]
pool.wait()
print '%d second' % (time.time() - start_time)