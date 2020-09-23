#!/usr/bin/python
# -*- coding: UTF-8 -*-

import time
import argparse
import os
import re
import subprocess
import socket
from textwrap import dedent
import MySQLdb as mysql
import sys
sys.path.append("/scripts/dataware_house/program/common")
import PyCommonFunc
import pyhs2


def get_host_ip():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('8.8.8.8', 80))
        ip = s.getsockname()[0]
    finally:
        s.close()

    return ip


def get_columninfo(mysql_host, username, password, mysql_tab, mysql_db, column_list=''):
    mysql_column_sql = ''
    if not column_list:
        mysql_column_sql = dedent("""\
                    SELECT CONCAT(column_name,
                                '=',
                                CASE data_type
                                    WHEN 'int' THEN
                                    'int'
                                    WHEN 'bigint' THEN
                                    'bigint'
                                    WHEN 'tinyint' THEN
                                    'int'
                                    WHEN 'double' THEN
                                    '''decimal(38%%2C10)'''
                                    WHEN 'decimal' THEN
                                    '''decimal(38%%2C10)'''
                                    WHEN 'varchar' THEN
                                    'String'
                                    ELSE
                                    'String'
                                END) aa
                    FROM information_schema.COLUMNS
                    WHERE lower(table_name) = '%s' AND lower(TABLE_SCHEMA) = '%s' ORDER BY ORDINAL_POSITION
                """ % (str(mysql_tab).lower(), str(mysql_db).lower()))
    else:
        column_list = ','.join(["'%s'" % i for i in column_list.split(',')])
        mysql_column_sql = dedent("""\
                SELECT CONCAT(column_name,
                            '=',
                            CASE data_type
                                WHEN 'int' THEN
                                'int'
                                WHEN 'bigint' THEN
                                'bigint'
                                WHEN 'tinyint' THEN
                                'int'
                                WHEN 'double' THEN
                                '''decimal(38%%2C10)'''
                                WHEN 'decimal' THEN
                                '''decimal(38%%2C10)'''
                                WHEN 'varchar' THEN
                                'String'
                                ELSE
                                'String'
                            END) aa
                FROM information_schema.COLUMNS
                WHERE lower(table_name) = '%s' AND lower(TABLE_SCHEMA) = '%s' 
                AND lower(COLUMN_NAME) in (%s) ORDER BY ORDINAL_POSITION
            """ % (str(mysql_tab).lower(), str(mysql_db).lower(), str(column_list).lower()))
    # print mysql_column_sql
    hive_columns = ''
    column_info = ()
    try:
        conn1 = mysql.connect(host=mysql_host, port=3306,
                              user=username, passwd=password, db=mysql_db, charset="utf8")
        cursor1 = conn1.cursor()
        cursor1.execute(mysql_column_sql)
        result1 = cursor1.fetchall()
        if not result1:
            # log_util.error('mysql table %s not exsited' % str(v_oratab).lower())
            print ('Mysql table %s.%s not exsited' %
                   (str(mysql_db).lower(), str(mysql_tab).lower()))
            sys.exit(1)
        for column in result1:
            column_info = column_info + column
        hive_columns = ','.join(column_info)
    except Exception, e:
        # log_util.error('mysql error: %s' % e)
        print 'Mysql error: %s' % e
    finally:
        cursor1.close()
        conn1.close()
    # log_util.info('hive columns [ %s ]' % str(hive_columns))
    return hive_columns


def print_sqoopcmd(column_str, jdbc, mysql_user, mysql_table, mysql_db, hive_db, hive_table, logpath, file_format='textfile', split_column='', map_num=1, where_condition='', part_key='', part_value='', column_list=''):
    sqoop_cmd_str = ''
    cmd_dict = {}
    job_name = 'sqoop_import_mysql2hive_%s.%s' % (
        str(mysql_db).lower(), str(mysql_table).lower())
    cmd_dict['jdbc'] = jdbc
    cmd_dict['job_name'] = job_name
    cmd_dict['mysql_user'] = str(mysql_user).upper()
    cmd_dict['mysql_table'] = str(mysql_table).upper()
    cmd_dict['hive_db'] = hive_db
    cmd_dict['hive_table'] = hive_table
    cmd_dict['file_format'] = file_format
    cmd_dict['logpath'] = logpath
    sqoop_hcatalog_base = dedent(("""sqoop import -D mapreduce.map.memory.mb=2048 -D mapreduce.map.java.opts=-Xmx1536m \\""" + '\n'
                             + '-D hive.exec.scratchdir=/user/impala/hive_scratchdir \\' + '\n'
                             + '-D mapred.job.name={job_name} \\' + '\n'
                             + """-D mapred.child.java.opts="-Djava.security.egd=file:/dev/../dev/urandom" \\""" + '\n'
                             + '--connect {jdbc} \\' + '\n'
                             + '--table {mysql_table} \\'+'\n'
                             + '--hcatalog-database {hive_db} --hcatalog-table {hive_table} \\' + '\n'
                             + """--hcatalog-storage-stanza 'stored as {file_format}' \\""" + '\n'
                             + """--null-string '\\\N' --null-non-string '\\\N' --hive-drop-import-delims \\""" + '\n'
                             + '--outdir {logpath} \\' + '\n'
                             + '--hive-overwrite --delete-target-dir \\' + '\n').format(**cmd_dict))

    sqoop_hiveimport_base = dedent(("""sqoop import -D mapreduce.map.memory.mb=2048 -D mapreduce.map.java.opts=-Xmx1536m \\""" + '\n'
                             + '-D hive.exec.scratchdir=/user/impala/hive_scratchdir \\' + '\n'
                             + '-D mapred.job.name={job_name} \\' + '\n'
                             + """-D mapred.child.java.opts="-Djava.security.egd=file:/dev/../dev/urandom" \\""" + '\n'
                             + '--hive-import --connect {jdbc} \\' + '\n'
                             + '--table {mysql_table} \\'+'\n'
                             + '--hive-database {hive_db} --hive-table {hive_table} \\' + '\n'
                             + """--as-{file_format} \\""" + '\n'
                             + """--null-string '\\\N' --null-non-string '\\\N' --hive-drop-import-delims \\""" + '\n'
                             + '--outdir {logpath} \\' + '\n'
                             + '--hive-overwrite --delete-target-dir \\' + '\n').format(**cmd_dict))
    
    sqoop_cmd_str = ''
    if file_format == 'parquet' or file_format == 'parquetfile':
        sqoop_cmd_str = sqoop_hcatalog_base + '--map-column-hive ' + column_str + ' \\' + '\n'
        hive_table_existed = True
        try:
            conn = pyhs2.connect(host='10.14.240.254', port=10182, authMechanism="PLAIN",
                                user='impala', password='impala_+-', database=hive_db)
            cursor = conn.cursor()
            cursor.execute("""show tables '%s'""" % hive_table)
            result = cursor.fetch()
            # print result
            if not result:
                hive_table_existed = False
        except Exception, e:
            print e
        finally:
            cursor.close()
            conn.close()
        # print hive_table_existed
        if not hive_table_existed:
            sqoop_cmd_str = sqoop_cmd_str + '--create-hcatalog-table \\' + '\n'

        if part_key:
            sqoop_cmd_str = sqoop_cmd_str + \
                '--hcatalog-partition-keys ' + part_key + ' \\' + '\n'

        if part_value:
            sqoop_cmd_str = sqoop_cmd_str + \
                '--hcatalog-partition-values ' + part_value + ' \\' + '\n'
    else:
        sqoop_cmd_str = sqoop_hiveimport_base + '--map-column-hive ' + column_str + ' \\' + '\n'
        if part_key:
            sqoop_cmd_str = sqoop_cmd_str + \
                '--hive-partition-key ' + part_key + ' \\' + '\n'

        if part_value:
            sqoop_cmd_str = sqoop_cmd_str + \
                '--hive-partition-value ' + part_value + ' \\' + '\n'

    if column_list:
        sqoop_cmd_str = sqoop_cmd_str + '--columns ' + column_list + ' \\' + '\n'

    if where_condition:
        sqoop_cmd_str = sqoop_cmd_str + """ --where \"""" + where_condition + """\""""

    if split_column:
        sqoop_cmd_str = sqoop_cmd_str + ' --split-by ' + \
            split_column + '--num-mappers ' + str(map_num)
    else:
        sqoop_cmd_str = sqoop_cmd_str + ' --num-mappers 1' 
    # log_util.info('sqoop_cmd: \n' + sqoop_cmd_str)
    # print 'sqoop_cmd: \n' + sqoop_cmd_str
    return sqoop_cmd_str


def exec_cmd(cmd, log_util):
    """执行sqoop"""
    r_code = 0
    # log = log_utils()
    try:
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT, close_fds=True)
        for line in iter(p.stdout.readline, ''):
            line = line.rstrip().decode('utf-8', "ignore")
            line = re.sub(r'\d+/\d+/\d+ \d+:\d+:\d+', '', line)
            if 'ERROR' in line or 'FAILED' in line:
                log_util.error(line)
                r_code = 1
            else:
                log_util.info(line)
    except Exception, e:
        r_code = 1
        log_util.error('Execute ERROR !!!')
        log_util.error(e)
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
            Usage: python sqoop_import_mysql2hive.py -h bi_tmp.t_test_table \\
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
    logpath = os.path.join(
        '/mnt/data0/logs/dataware_house/log/program/', str(v_sysdate))

    parser = argparse.ArgumentParser(
        description="Sync mysql data to hive table", conflict_handler='resolve')
    parser.add_argument('-h', '--hivetable', required=True,
                        help="指定hive的库名.表名, example: bi_tmp.t_test_tab")
    parser.add_argument('-o', '--mysqltable', required=True,
                        help='指定mysql的表名, example: t_game_info')
    parser.add_argument('-b', '--base', required=True,
                        help='指定mysql的database, example: log_verification')
    parser.add_argument('-w', '--where', default='',
                        help="""指定mysql查询的where条件,必须用双引号包起来, example: \"logtime=date'2020-05-14 and groupingid=128'\"""")
    parser.add_argument('-f', '--fileformat', default='textfile',
                        help='指定hive表的存储格式, example: parquet')
    parser.add_argument('-s', '--split', default='',
                        help="指定mysql表的分隔字段,必须是date或number, example: logtime")
    parser.add_argument('-m', '--mapnum', default=1,
                        help="指定运行时的map数量,必须与-s一起使用, example: -m 2")
    parser.add_argument('-p', '--partition', default='',
                        help="""指定hive表的分区字段和分区名称,必须用\"包起来, example:par_dt=2020-05-14""")
    parser.add_argument('-c', '--columns', default='', 
                        help="指定需要导出的列名,用逗号分隔, example:'userid,groupname,logtime'")                        
    args = parser.parse_args()
    hive_tablename = args.hivetable
    hive_db = str(hive_tablename).split('.')[0]
    hive_table = str(hive_tablename).split('.')[1]
    mysql_table = args.mysqltable
    logname = 'sqoop_import_' + str(mysql_table).lower() + '.log'
    logfile = os.path.join(logpath, logname)
    mysql_base = args.base
    where_condition = args.where
    file_format = args.fileformat
    split_column = args.split
    map_num = args.mapnum
    partition_info = args.partition
    column_list = args.columns
    keys = []
    values = []
    partition_keys = ''
    partition_values = ''
    if partition_info:
        keys = [i.split('=')[0] for i in str(partition_info).split(',')]
        values = [i.split('=')[1] for i in str(partition_info).split(',')]
        partition_keys = ','.join(keys)
        partition_values = ','.join(values)

    if not os.path.exists(logpath):
        os.makedirs(logpath)
    # log_util = log_utils(logfile)
    log_util = PyCommonFunc.LogUtil(logfile)
    mysql_user = 'log_verification'
    mysql_password = 'log_verification2020'
    mysql_host = '10.14.240.254'
    mysql_port = '3306'
    jdbc_link = 'jdbc:mysql://%s:%s/%s --username %s --password %s' % \
                (mysql_host, mysql_port, mysql_base, mysql_user, mysql_password)
    columns = get_columninfo(mysql_host=mysql_host, username=mysql_user,
                             password=mysql_password, mysql_tab=mysql_table, mysql_db=mysql_base, column_list=column_list)
    sqoop_command_str = print_sqoopcmd(column_str=columns,
                                       jdbc=jdbc_link,
                                       mysql_user=mysql_user,
                                       mysql_table=mysql_table,
                                       mysql_db=mysql_base,
                                       hive_db=hive_db,
                                       hive_table=hive_table,
                                       logpath=logpath,
                                       file_format=file_format,
                                       split_column=split_column,
                                       map_num=map_num,
                                       where_condition=where_condition,
                                       part_key=partition_keys,
                                       part_value=partition_values,
                                       column_list=column_list)
    # print command_str
    HDFS_PATH = '/user/hive/warehouse'
    table_path = os.path.join(HDFS_PATH, str(
        hive_db).lower()+'.db', str(hive_table).lower())
    partition_path = partition_info.replace("'", "").replace(",", "/")
    hive_table_path = os.path.join(HDFS_PATH, table_path, partition_path, '*')
    drop_hdfs_file = 'hdfs dfs -rm ' + hive_table_path
    ip = get_host_ip()
    log_util.info("Task begin.......")
    log_util.info("Sqoop command :\n" + sqoop_command_str)
    if file_format == 'parquet' or file_format == 'parquetfile':
        log_util.info("drop hive historical data: \n" + drop_hdfs_file)
        exec_cmd(drop_hdfs_file, log_util)
        impl = PyCommonFunc.ImpalaUtil()
        impl.refresh_table_nopart(hive_tablename)
        impl._close()
    # 开始执行sqoop命令
    rtcode = exec_cmd(sqoop_command_str, log_util)
    log_util.info('logfile:' + logfile)
    log_util.info('executor IP:' + ip)
    log_util.info('{table:%s, partition:(%s)}' %
                      (hive_tablename, partition_info))
    
    if rtcode > 0:
        log_util.error('*** FAIL ***')
        sys.exit(1)
    else:
        impl = PyCommonFunc.ImpalaUtil()
        impl.invalidate_metadata(hive_tablename)
        impl.refresh_table_nopart(hive_tablename)
        impl._close()
        log_util.info('*** SUCCESS ***')
        sys.exit(0)


if __name__ == '__main__':
    main()
