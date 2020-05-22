#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
@author:cwj
@time:2020-05-18
功能: 使用sqoop将Oracle的数据同步到hive
参数:
     -h [hivedb.hivetable] hive的表名
     -o [oracle_tablename] Oracle表名
     -b [oracle_basename] oracle_connect.config配置的Oracle连接标识
     -w [oracle sql where condition] oracle查询的where 条件，必须用双引号括起来
     -m [map_num] map的数量，可选参数
     -f [fileformat] hive表的存储格式,默认是textfile，可选orc,parquet
     -s [split_columnname] 切分数据的列名，可选参数，应该选取时间类型或数值类型的列
     -p [partition] hive表的分区信息，只支持分区键是string，支持多级分区，必须用双引号括起来,exp:-p "par_dt=2020-04-01,par_game=138"
usage: python sqoop_import_oracle2hive.py -h bi_tmp.t_000073_log_rolelogin_test -o t_000073_log_rolelogin -b new_db \
       -w "logtime>=date'2019-01-09' and logtime<date'2019-01-12'" -m 3 -s userid_
注意：-b值要与配置文件/scripts/dataware_house/cfg/oracle_connect.config对应
     -m 与 -s 是可选参数，必须同时使用
     -w 可选参数，Oracle的where条件，使用时后面直接写条件，不用带where关键字，且整个条件必须用双引号
     hive的表不存在会自动创建，如果已存在会覆盖旧数据。
"""

import pyhs2
import cx_Oracle as cx
import sys
import argparse
sys.path.append("/scripts/dataware_house/program/common")
import PyCommonFunc
import logging
import os
import re
import time
import subprocess
import socket
from textwrap import dedent

def get_host_ip():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('8.8.8.8', 80))
        ip = s.getsockname()[0]
    finally:
        s.close()

    return ip

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

    def __init__(self, logfile):
        self.logfilter = NoParsingFilter()
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
        self.formatter = logging.Formatter('%(asctime)s - %(levelname)s: %(message)s')
        self.fh.setFormatter(self.formatter)
        self.ch.setFormatter(self.formatter)
        self.logger.addHandler(self.fh)
        self.logger.addHandler(self.ch)
        self.logger.addFilter(self.logfilter)

    def log_info(self, info_mess):
        self.logger.info(info_mess)

    def log_error(self, error_mess):
        self.logger.error(error_mess)

def get_columninfo(cx_ora_link, v_oratab):
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
    column_info = ()
    try:
        conn1 = cx.connect(cx_ora_link)
        cursor1 = conn1.cursor()
        cursor1.prepare(ora_sql)
        cursor1.execute(None, {'tab_name': str(v_oratab).lower()})
        result1 = cursor1.fetchall()
        if not result1:
            # log_util.log_error('oracle table %s not exsited' % str(v_oratab).lower())
            print ('oracle table %s not exsited' % str(v_oratab).lower())
            sys.exit(1)
        for column in result1:
            column_info = column_info + column
        hive_columns = ','.join(column_info)
    except Exception, e:
        # log_util.log_error('oracle error: %s' % e)
        print 'oracle error: %s' % e
    finally:
        cursor1.close()
        conn1.close()
    # log_util.log_info('hive columns [ %s ]' % str(hive_columns))
    return hive_columns


def print_sqoopcmd(column_str, jdbc, oracle_user, oracle_table, hive_db, hive_table, logpath, file_format='textfile', split_column='', map_num=1, where_condition='', part_key='', part_value=''):
    sqoop_cmd_str = ''
    cmd_dict = {}
    job_name = 'sqoop_import_%s.%s' % (oracle_user, oracle_table)
    cmd_dict['jdbc'] = jdbc
    cmd_dict['job_name'] = job_name
    cmd_dict['oracle_user'] = str(oracle_user).upper()
    cmd_dict['oracle_table'] = str(oracle_table).upper()
    cmd_dict['hive_db'] = hive_db
    cmd_dict['hive_table'] = hive_table
    cmd_dict['file_format'] = file_format
    cmd_dict['logpath'] = logpath
    sqoop_str_base = dedent(("""sqoop import -D mapreduce.map.memory.mb=2048 -D mapreduce.map.java.opts=-Xmx1536m \\""" + '\n'
        + '-D hive.exec.scratchdir=/user/impala/hive_scratchdir \\' + '\n'
        + '-D mapred.job.name={job_name} \\' + '\n'
        + """-D mapred.child.java.opts="-Djava.security.egd=file:/dev/../dev/urandom" \\""" + '\n'
        + '--connect {jdbc} \\' + '\n'
        + '--table {oracle_user}.{oracle_table} \\'+'\n'
        + '--hcatalog-database {hive_db} --hcatalog-table {hive_table} \\' + '\n'
        + """--hcatalog-storage-stanza 'stored as {file_format}' \\""" + '\n'
        + """--null-string '\\\N' --null-non-string '\\\N' --hive-drop-import-delims \\""" + '\n'
        + '--outdir {logpath} \\' + '\n'
        + '--hive-overwrite --delete-target-dir \\' + '\n').format(**cmd_dict))

    sqoop_cmd_str = sqoop_str_base + '--map-column-hive ' + column_str + ' \\' + '\n'
    hive_table_existed = True
    try:
        conn = pyhs2.connect(host='10.14.240.254',port=10182,authMechanism="PLAIN",user='impala',password='impala_+-',database=hive_db)
        cursor = conn.cursor()
        cursor.execute("""show tables '%s'""" % hive_table)
        result = cursor.fetch()
        # print result
        if not result:
            hive_table_existed = False
    except Exception,e:
        print e 
    finally:
        cursor.close()
        conn.close()
    # print hive_table_existed
    if not hive_table_existed:
        sqoop_cmd_str = sqoop_cmd_str + '--create-hcatalog-table \\' + '\n'

    if part_key:
        sqoop_cmd_str = sqoop_cmd_str + '--hcatalog-partition-keys ' + part_key + ' \\' + '\n'

    if part_value:
        sqoop_cmd_str = sqoop_cmd_str + '--hcatalog-partition-values ' + part_value + ' \\' + '\n'

    if where_condition:
        sqoop_cmd_str = sqoop_cmd_str + """ --where \"""" + where_condition + """\""""

    if split_column:
        sqoop_cmd_str = sqoop_cmd_str + ' --split-by ' + split_column + ' --num-mappers ' + str(map_num)
    else:
        sqoop_cmd_str = sqoop_cmd_str + ' --num-mappers 1'

    # log_util.log_info('sqoop_cmd: \n' + sqoop_cmd_str)
    # print 'sqoop_cmd: \n' + sqoop_cmd_str
    return sqoop_cmd_str

def exec_cmd(cmd, log_util):
    """执行sqoop"""
    r_code = 0
    # log = log_utils()
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
        log_util.log_error('Execute ERROR !!!')
        log_util.log_error(e)
        sys.exit(1)
    finally:
        p.stdout.close()
        p.wait()
    # print 'Return Code of Sqoop Execute :',str(r_code)
    return r_code    


def main():
    if len(sys.argv[1:]) < 1:
        print dedent("""\
            Warning : 参数不能为空
            Usage: python sqoop_import_oracle2hive.py -h bi_tmp.t_test_table \\
                                                      -o t_game_info \\
                                                      -b dt_sdb \\
                                                      -w \"to_number(gameid)<=138\" \\
                                                      -s gameid \\
                                                      -m 2 \\
                                                      -f parquet \\
                                                      -p \"par_dt='2020-05-17'\" """)
        sys.exit(1)
    v_sysdate = time.strftime("%Y%m%d", time.localtime())
    # logpath = '/mnt/data0/logs/dataware_house/log/program/' + str(v_sysdate) + '/'
    logpath = os.path.join('/mnt/data0/logs/dataware_house/log/program/', str(v_sysdate))

    parser = argparse.ArgumentParser(description="Sync oracle data to hive table",conflict_handler='resolve')
    parser.add_argument('-h','--hivetable', required=True, help="指定hive的库名.表名, example: bi_tmp.t_test_tab")
    parser.add_argument('-o','--oracletabe', required=True, help='指定Oracle的表名, example: t_game_info')
    parser.add_argument('-b','--base', required=True, help='指定Oracle的连接标识, example: new_db')
    parser.add_argument('-w','--where', default='', help="""指定Oracle查询的where条件,必须用双引号包起来, example: \"logtime=date'2020-05-14 and groupingid=128'\"""")
    parser.add_argument('-f','--fileformat', default='textfile', help='指定hive表的存储格式, example: parquet')
    parser.add_argument('-s','--split', default='', help="指定Oracle表的分隔字段,必须是date或number, example: logtime")
    parser.add_argument('-m','--mapnum', default=1, help="指定运行时的map数量,必须与-s一起使用, example: -m 2")
    parser.add_argument('-p','--partition', default='', help="""指定hive表的分区字段和分区名称,必须用\"包起来, example:par_dt=2020-05-14""")
    args = parser.parse_args()
    hive_tablename=args.hivetable
    hive_db=str(hive_tablename).split('.')[0]
    hive_table=str(hive_tablename).split('.')[1]
    oracle_table=args.oracletabe
    logname = 'sqoop_import_' + str(oracle_table).lower() + '.log'
    logfile = os.path.join(logpath, logname)
    oracle_base=args.base
    where_condition=args.where
    file_format=args.fileformat
    split_column=args.split
    map_num=args.mapnum
    partition_info=args.partition
    keys=[]
    values=[]
    partition_keys=''
    partition_values=''
    if partition_info:
        keys=[i.split('=')[0] for i in str(partition_info).split(',')]
        values=[i.split('=')[1] for i in str(partition_info).split(',')]
        partition_keys=','.join(keys)
        partition_values=','.join(values)

    if not os.path.exists(logpath):
        os.makedirs(logpath)
    log_util = log_utils(logfile)
    
    (rs, op, dest_db_info) = PyCommonFunc.get_dest_db_info(oracle_base)
    jdbc_link = 'jdbc:oracle:thin:@%s:%s/%s -username %s -password %s' % \
                (dest_db_info[2], dest_db_info[3], dest_db_info[4], dest_db_info[0], dest_db_info[1])
    cx_ora_link = '%s/%s@%s:%s/%s' % \
                  (dest_db_info[0], dest_db_info[1], dest_db_info[2], dest_db_info[3], dest_db_info[4])
    ora_user = dest_db_info[0]
    
    columns=get_columninfo(cx_ora_link, oracle_table)
    sqoop_command_str=print_sqoopcmd(column_str = columns,
                               jdbc = jdbc_link,
                               oracle_user = ora_user,
                               oracle_table = oracle_table,
                               hive_db = hive_db,
                               hive_table = hive_table,
                               logpath = logpath,
                               file_format = file_format,
                               split_column = split_column,
                               map_num = map_num,
                               where_condition = where_condition,
                               part_key=partition_keys,
                               part_value=partition_values)
    # print command_str
    HDFS_PATH='/user/hive/warehouse'
    table_path=os.path.join(HDFS_PATH,str(hive_db).lower()+'.db',str(hive_table).lower())
    partition_path=partition_info.replace("'","").replace(",","/")
    hive_table_path=os.path.join(HDFS_PATH,table_path,partition_path,'*')
    drop_hdfs_file = 'hdfs dfs -rm ' + hive_table_path
    ip = get_host_ip()
    log_util.log_info("Task begin.......")
    log_util.log_info("Sqoop command :\n" + sqoop_command_str)
    log_util.log_info("drop hive historical data: \n" + drop_hdfs_file)
    exec_cmd(drop_hdfs_file, log_util)
    # 开始执行sqoop命令
    rtcode = exec_cmd(sqoop_command_str, log_util)
    log_util.log_info('logfile:' + logfile)
    log_util.log_info('executor IP:' + ip)
    log_util.log_info('{table:%s,partition:(%s)}' % (hive_tablename, partition_info))
    impl = PyCommonFunc.ImpalaUtil()
    if rtcode > 0:
        log_util.log_error('*** FAIL ***')
        sys.exit(1)
    else:
        impl.invalidate_metadata(hive_tablename)
        impl.refresh_table_nopart(hive_tablename)
        log_util.log_info('*** SUCCESS ***')
        sys.exit(0)

if __name__ =='__main__':
    main()    