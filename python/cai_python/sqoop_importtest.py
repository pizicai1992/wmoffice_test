#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
@author:cwj
@time:2019-04-19
功能: 使用sqoop将Oracle的数据同步到hive
参数:
     -h [hivedb.hivetable] hive的表名
     -o [oracle_tablename] Oracle表名
     -b [oracle_basename] oracle_connect.config配置的Oracle连接标识
     -w [oracle sql where condition] oracle查询的where 条件，必须用双引号括起来
     -m [map_num] map的数量，可选参数
     -s [split_columnname] 切分数据的列名，可选参数
usage: python sqoop_test5.py -h bi_tmp.t_000073_log_rolelogin_test -o t_000073_log_rolelogin -b new_db -w "logtime>=date'2019-01-09' and logtime<date'2019-01-12'" -m 3 -s userid_
注意：-b值要与配置文件/scripts/dataware_house/cfg/oracle_connect.config对应
     -m 与 -s 是可选参数，必须同时使用
     -w 可选参数，Oracle的where条件，使用时后面直接写条件，不用带where关键字，且整个条件必须用双引号
"""

import pyhs2
import cx_Oracle as cx
import sys, getopt

sys.path.append("/scripts/dataware_house/program/common")
import PyCommonFunc
import logging
import os, re
import time
import subprocess
import socket

if len(sys.argv[1:]) < 1:
    print 'ERROR : 参数不能为空'
    print 'Usage: python sqoop_import_oracle2hive.py -h hive_table -o oracle_tab \
    -b oracle_base -m mapnum -s splitcolumn'
    sys.exit(1)
opts, args = getopt.getopt(sys.argv[1:], '-h:-o:-b:-m:-s:-w:',
                           ['hive_table=', 'oracle_table=', 'oracle_base=', 'map=', 'split=', 'where='])
global v_hivetable, v_hivetab, v_hivedb, v_oratab, v_oracle_base, v_date, v_map, v_split, v_file, v_outfile,v_where
v_map = 1
v_where = None
v_split = None

try:
    for opt_name, opt_value in opts:
        if opt_name in ('-h', '-hive_table'):
            # global v_hivetable
            # global v_hivetab
            # global v_hivedb
            v_hivetable = opt_value
            v_hivetab = str(v_hivetable).split('.')[1]
            v_hivedb = str(v_hivetable).split('.')[0]
        if opt_name in ('-o', '-oracle_table'):
            # global v_oratab
            v_oratab = opt_value
        if opt_name in ('-b', '-oracle_base'):
            # global v_oracle_base
            oracle_base = opt_value
        if opt_name in ('-m', '-map'):
            v_map = opt_value
        if opt_name in ('-s', '-split'):
            v_split = opt_value
        if opt_name in ('-w', '-where'):
            v_where = opt_value

except getopt.GetoptError:
    print("ERROR : 参数错误")
    print 'Usage: 请输入 -h hive_table -o oracle_tab -b oracle_base -m mapnum -s splitcol -w "wherecondition"'
    sys.exit(1)

v_sysdate = time.strftime("%Y%m%d", time.localtime())
logpath = '/mnt/data0/logs/dataware_house/log/program/' + str(v_sysdate) + '/'
logfile = logpath + 'sqoop_import_' + str(v_oratab).lower() + '.log'
def get_host_ip():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('8.8.8.8', 80))
        ip = s.getsockname()[0]
    finally:
        s.close()

    return ip

ips=get_host_ip()

class NoParsingFilter(logging.Filter):
    """日志过滤器"""

    def filter(self, record):
        if record.getMessage().startswith('Warning'):
            return False
        elif record.getMessage().find('Adding to job classpath') >= 1:
            return False
        elif record.getMessage().find('files under') >= 1:
            return False
        return True


class log_utils():
    """日志记录"""

    def __init__(self):
        self.logfilter = NoParsingFilter()
        self.log_path = logpath
        self.log_file = logfile
        # self.log_file = self.log_path + 'sqoop_export_' + str(v_hivetab).lower() + '.log'
        # self.log_mess = str(log_message)
        if os.access(self.log_file, os.F_OK):
            os.remove(self.log_file)
        self.logger = logging.getLogger('sqoop_import_logger')
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

    def log_info(self, info_mess):
        self.logger.info(info_mess)

    def log_error(self, error_mess):
        self.logger.error(error_mess)


log_util = log_utils()

log_util.log_info('************* Task Begin ****************')
# 1.获得JDBC连接串
(rs, op, dest_db_info) = PyCommonFunc.get_dest_db_info(oracle_base)
jdbc_link = 'jdbc:oracle:thin:@%s:%s/%s -username %s -password %s' % \
            (dest_db_info[2], dest_db_info[3], dest_db_info[4], dest_db_info[0], dest_db_info[1])
cx_ora_link = '%s/%s@%s:%s/%s' % \
              (dest_db_info[0], dest_db_info[1], dest_db_info[2], dest_db_info[3], dest_db_info[4])
ora_user = dest_db_info[0]

# 2.sqoop基础命令
sqoop_str1 = ''
sqoop_str_base = ("""
sqoop import -D mapreduce.map.memory.mb=2048 -D mapreduce.map.java.opts=-Xmx1536m """ + ' \\' + '\n'
+ """-D hive.exec.scratchdir=/user/impala/hive_scratchdir""" + ' \\' + '\n'
+ """-D mapred.job.name=sqoop_""" + str(ora_user).lower() + ' \\' + '\n'
+ """-D mapred.child.java.opts="-Djava.security.egd=file:/dev/../dev/urandom" """ + ' \\' + '\n'
+ '--hive-import --connect ' + jdbc_link + ' \\' + '\n'
+ '--hive-database ' + v_hivedb.lower() + ' \\' + '\n'
+ '--hive-table ' + v_hivetab.lower() + ' \\' + '\n'
+ '--table ' + str(ora_user).upper() + '.' + str(v_oratab).upper() + ' \\' + '\n'
+ """--input-null-string '\\\N' --input-null-non-string '\\\N' --hive-drop-import-delims""" + ' \\' + '\n'
+ '--outdir ' + logpath + ' \\' + '\n'
'--hive-overwrite --delete-target-dir' + ' \\' + '\n')

# 3. 获得Oracle列名与hive字段类型
def get_columninfo():
    ora_sql = """
            select column_name || '=' || decode(data_type,
                                                    'NUMBER',
                                                    '''decimal(38%2C10)''',
                                                    'FLOAT',
                                                    '''decimal(38%2C10)''',
                                                    'DOUBLE',
                                                    '''decimal(38%2C10)''',
                                                    'VARCHAR2',
                                                    'String',
                                                    'DATE',
                                                    'String',
                                                    'String')
                  from user_tab_columns
                 where lower(table_name) = :tab_name
                 order by column_id
            """
    hive_columns = ''
    column = ()
    try:
        conn1 = cx.connect(cx_ora_link)
        cursor1 = conn1.cursor()
        cursor1.prepare(ora_sql)
        cursor1.execute(None, {'tab_name': str(v_oratab).lower()})
        result1 = cursor1.fetchall()
        if not result1:
            log_util.log_error('oracle table %s not exsited' % str(v_oratab).lower())
            sys.exit(1)
        for resu in result1:
            column = column + resu
        hive_columns = ','.join(column)
        print hive_columns
    except Exception, e:
        log_util.log_error('oracle error: %s' % e)
    finally:
        cursor1.close()
        conn1.close()
    print hive_columns
    return hive_columns

# 4. 拼接完整的sqoop命令
def print_sqoopcmd():

    column_str = get_columninfo()
    sqoop_cmd_str = sqoop_str_base + ' --map-column-hive ' + column_str + ' \\' + '\n'
    if v_split:
        sqoop_cmd_str = sqoop_cmd_str + ' --split-by ' + v_split + ' --num-mappers ' + str(v_map)
    else:
        sqoop_cmd_str = sqoop_cmd_str + ' --num-mappers 1'

    if v_where:
        sqoop_cmd_str = sqoop_cmd_str + """ --where \"""" + v_where + """\""""

    log_util.log_info('sqoop_cmd: \n' + sqoop_cmd_str)
    return sqoop_cmd_str

# 5. 执行sqoop命令
def exec_sqoop():
    """执行sqoop"""
    cmd = print_sqoopcmd()
    global r_code
    r_code = 0
    try:
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,close_fds=True)
        for line in iter(p.stdout.readline, ''):
            line = line.rstrip().decode('utf-8', "ignore")
            line = re.sub('\d+/\d+/\d+ \d+:\d+:\d+','',line)
            if 'ERROR' in line or 'FAILED' in line:
                log_util.log_error(line)
                r_code = 1
            else:
                log_util.log_info(line)
    except Exception, e:
        r_code = 1
        log_util.log_error('Sqoop Execute ERROR !!!')
        log_util.log_error(e)
        sys.exit(1)
    finally:
        p.stdout.close()
        p.wait()
    print 'Return Code of Sqoop Execute :',str(r_code)
    return r_code

def main():
    rd = exec_sqoop()
    log_util.log_info('executor IP: ' + ips)
    log_util.log_info('logfile:' + log_util.log_file)
    if rd == 0:
        log_util.log_info('*** SUCCESS ***')
        sys.exit(0)
    else:
        log_util.log_error('*** FAIL ***')
        sys.exit(1)

if __name__ =='__main__':
    main()