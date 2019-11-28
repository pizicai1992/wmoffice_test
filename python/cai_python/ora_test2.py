#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
@author:cwj
@time:2018-10-22
功能: 使用sqoop同步Oracle的字典表到hive
配置表: quartz3.t_dic_sqoop_testcai
参数: -n [3] 同时执行sqoop的线程数
usage: python sqoop_dictable.py -n 3
"""

import cx_Oracle as cx
import ConfigParser
import logging
import os,sys
import time
import threadpool
from optparse import OptionParser

class NoParsingFilter(logging.Filter):
    """日志过滤器"""
    def filter(self, record):
        return not record.getMessage().startswith('Warning')

#logger.addFilter(NoParsingFilter())

class log_utils():
    """日志记录"""
    def __init__(self):
        self.logfilter = NoParsingFilter()
        self.log_path = '/mnt/data0/logs/dataware_house/log/'
        self.log_file = self.log_path+'sqoop_syn_dictable.log'
        #self.log_mess = str(log_message)
        if os.access(self.log_file, os.F_OK):
            os.remove(self.log_file)
        self.logger = logging.getLogger('sqoop_syn_dictable_logger')
        self.logger.setLevel(logging.INFO)
        self.fh = logging.FileHandler(self.log_file)
        self.fh.setLevel(logging.INFO)
        self.ch = logging.StreamHandler()
        self.ch.setLevel(logging.INFO)
        self.formatter = logging.Formatter('%(asctime)s - %(levelname)s :%(message)s')
        self.fh.setFormatter(self.formatter)
        self.ch.setFormatter(self.formatter)
        self.logger.addHandler(self.fh)
        self.logger.addHandler(self.ch)
        self.logger.addFilter(self.logfilter)

    def log_info(self,info_mess):
        self.logger.info(info_mess)

    def log_error(self,error_mess):
        self.logger.error(error_mess)

log_util = log_utils()

class get_connect():
    """获得sqoop和cx_Oracle的连接串以及Oracle的用户名"""

    def __init__(self, ora_db):
        config = ConfigParser.RawConfigParser()
        self.conf_file = '/scripts/dataware_house/cfg/oracle_connect.config'
        config.read(self.conf_file)
        self.ora_username = str(config.get(ora_db, 'username'))
        self.ora_password = str(config.get(ora_db, 'password'))
        self.ora_ip = str(config.get(ora_db, 'ip'))
        self.ora_host = str(config.get(ora_db, 'host'))
        self.ora_servname = str(config.get(ora_db, 'service_name'))

    def jdbc_conn(self):
        jdbc_link = 'jdbc:oracle:thin:@%s:%s/%s -username %s -password %s' % \
                    (self.ora_ip, self.ora_host, self.ora_servname, self.ora_username, self.ora_password)
        return jdbc_link

    def cx_ora_conn(self):
        cx_oar_link = '%s/%s@%s:%s/%s' % \
                      (self.ora_username, self.ora_password, self.ora_ip, self.ora_host, self.ora_servname)
        return cx_oar_link

    def ora_users(self):
        ora_user = self.ora_username
        return ora_user

sqoop_cmd_list = []

def print_sqoop_cmd(ora_dbname, oracle_tab, hive_db, hive_tab):
    """拼接sqoop的执行命令"""
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

    try:
        conn1 = cx.connect(ora_conct)
        cursor1 = conn1.cursor()
        cursor1.prepare(sql_str)
        cursor1.execute(None, {'tab_name': str(oracle_tab).lower()})
        result1 = cursor1.fetchall()
        if len(result1) > 0:
            log_util.log_info('Oracle字典表 %s 字段类型为' % str(oracle_tab).lower())
            log_util.log_info(result1)
        else:
            log_util.log_error('Oracle字典表 %s 获取不到字段类型' % str(oracle_tab).lower())
    except Exception, e:
        log_util.log_error('Oracle数据库错误: ')
        log_util.log_error(e)
    finally:
        cursor1.close()
        conn1.close()

    for resu in result1:
        column = column + resu

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
    """执行sqoop命令"""
    try:
        log_util.log_info('sqoop_cmd is :'+'\n'+cmd_str)
        result = os.popen(cmd_str)
        log_util.log_info(result.read())
    except Exception, e:
        log_util.log_error('LOGFILE:'+log_util.log_file)
        log_util.log_error(e)
        raise
        sys.exit(1)


get_oraconn = get_connect('dt_sdb')
oracle_link = get_oraconn.cx_ora_conn()
conn = cx.connect(oracle_link)
cursor = conn.cursor()
cursor.execute("select * from t_dic_sqoop_testcai where valid=1")
result = cursor.fetchall()
cursor.close()
conn.close()
for res in result:
    print res[0], res[1], res[2], res[3]
    sqlss = print_sqoop_cmd(res[0],res[1],res[2],res[3])
    sqoop_cmd_list.append(sqlss)
    print
log_util.log_info('sqoop_cmd_list is :')
log_util.log_info(sqoop_cmd_list)
start_time = time.time()
usage = 'usage: python sqoop_dictable.py -n 3'
parser = OptionParser(usage=usage)
parser.add_option("-n", "--num", dest = "num", help = "并行导数的线程数")
(options, args) = parser.parse_args()
thread_num = int(options.num)

"""将参数列表和执行函数放入线程池并发执行"""
pool = threadpool.ThreadPool(thread_num)
requests = threadpool.makeRequests(exec_cmd, sqoop_cmd_list)
[pool.putRequest(req) for req in requests]
pool.wait()
log_util.log_info('LogFile:'+log_util.log_file)
log_util.log_info('TASK SUCCESS')
print 'task runtime %d second' % (time.time() - start_time)