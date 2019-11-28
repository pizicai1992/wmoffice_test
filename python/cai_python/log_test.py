#!/usr/bin/python
# -*- coding: UTF-8 -*-
import ConfigParser
import logging
import cx_Oracle as cx
import os
import subprocess
import chardet

class log_utils():
    def __init__(self):
        self.log_path = 'E://download_soft//'
        self.log_file = self.log_path + 'sqoop_syn_dictable.log'
        # self.log_mess = str(log_message)
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

    def log_info(self, info_mess):
        self.logger.info(info_mess)

    def log_error(self, error_mess):
        self.logger.error(error_mess)

logsss = log_utils()

def run_comd(command, print_msg=True):
    p = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

    lines = []
    for line in iter(p.stdout.readline, b''):
        #print chardet.detect(line)
        line = line.rstrip().decode('gbk',"ignore")
        if print_msg:
            logsss.log_info(line)
        lines.append(line)
    p.wait()
    return lines

class get_connect():
    '获得sqoop和cx_Oracle的连接串以及Oracle的用户名'

    def __init__(self, ora_db):
        config = ConfigParser.RawConfigParser()
        self.conf_file = r'E:\datawarehouse\datawarehouse\cfg\oracle_connect.config'
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
        print result1
        if len(result1) > 0:
            logsss.log_info('Oracle字典表 %s 获取字段类型成功' % str(oracle_tab).lower())
        else:
            logsss.log_error('Oracle字典表 %s 获取字段类型错误' % str(oracle_tab).lower())

    except Exception, e:
        logsss.log_error('Oracle 数据库错误：')
        logsss.log_error(e)
    finally:
        cursor1.close()
        conn1.close()
    # cursor1.prepare(sql_str)
    # cursor1.execute(None, {'tab_name': str(oracle_tab).lower()})
    # result1 = cursor1.fetchall()
    # cursor1.close()
    # conn1.close()

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

logsss.log_info('hello world')
logsss.log_error('fuck world')
print run_comd("ping www.baidu.com")

#print print_sqoop_cmd('new_db','t_dw_zxx_dic_serverlist', 'bi_tmp' ,'t_dim_zx_dic_serverlist')
