#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
@author:cwj
@time:2019-02-26
功能: 使用sqoop将hive表数据导入Oracle
参数: -h [hive表名:hivedb.hive_table] -o [oracle_tab] -b [oracle_base Oracle连接配置中的标签名] -d date -m map数量
usage: python sqoop_export_hive2oracle.py -h hive_table -o oracle_tab -b oracle_base -d date -m mapnum
注意: 参数-b的值要与配置文件/scripts/dataware_house/cfg/oracle_connect.config对应
     如果hive表不是分区表,则可以不加参数-d
     -m map数量设置过大无意义,最大map数为相应目录下的block数，默认为1
     oracle的目标表需要提前创建好
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

if len(sys.argv[1:]) < 1:
    print 'ERROR : 参数不能为空'
    print 'Usage: python sqoop_export_hive2oracle.py -h hive_table -o oracle_tab -b oracle_base -d date -m mapnum'
    sys.exit(1)
opts, args = getopt.getopt(sys.argv[1:], '-h:-o:-b:-d:-m:', ['hive_table=', 'oracle_table=', 'oracle_base=', 'date=', 'map='])
global v_hivetable, v_hivetab, v_hivedb, v_oratab, v_oracle_base, v_date, v_map
v_map = 1
v_date = None
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
        if opt_name in ('-d', '-date'):
            # global v_date
            v_date = opt_value
        if opt_name in ('-m', '-map'):
            v_map = opt_value
except getopt.GetoptError:
    print("ERROR : 参数错误")
    print 'Usage: 请输入 -h hive_table -o oracle_tab -b oracle_base -d date -m mapnum'
    sys.exit(1)

v_sysdate = time.strftime("%Y%m%d", time.localtime())
logpath = '/mnt/data0/logs/dataware_house/log/program/' + str(v_sysdate) + '/'
logfile = logpath + 'sqoop_export_' + str(v_hivetab).lower() + '.log'

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
        self.log_file = self.log_path + 'sqoop_export_' + str(v_hivetab).lower() + '.log'
        # self.log_mess = str(log_message)
        if os.access(self.log_file, os.F_OK):
            os.remove(self.log_file)
        self.logger = logging.getLogger('sqoop_export_logger')
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
# 1. 判断表的字段分隔符、是否分区表
conn = pyhs2.connect(host='192.168.10.10', port=10182, authMechanism="PLAIN", user='testuser')
cur = conn.cursor()
hivesql = "desc formatted " + str(v_hivetable)
try:
    cur.execute(hivesql)
except Exception, e:
    log_util.log_error('desc hive table %s ERROR!' % str(v_hivetable))
    log_util.log_error(e)
    log_util.log_info('logfile is: %s' % str(logfile))
    sys.exit(1)
else:
    descresult = cur.fetch()
    log_util.log_info('desc hive table %s SUCCESS!' % str(v_hivetable))
finally:
    cur.close()
    conn.close()

is_hivetab_partitioned = False
fields_terminat = r'\001'
for i in descresult:
    if 'field.delim         ' in i:
        log_util.log_info('fields-terminated-by: ' + str(i[2]).strip())
        fields_terminat = str(i[2]).strip()
    if '# Partition Information' in i:
        is_hivetab_partitioned = True

# 2. 获得JDBC连接串

(rs, op, dest_db_info) = PyCommonFunc.get_dest_db_info(oracle_base)
jdbc_link = 'jdbc:oracle:thin:@%s:%s/%s -username %s -password %s' % \
            (dest_db_info[2], dest_db_info[3], dest_db_info[4], dest_db_info[0], dest_db_info[1])
cx_ora_link = '%s/%s@%s:%s/%s' % \
              (dest_db_info[0], dest_db_info[1], dest_db_info[2], dest_db_info[3], dest_db_info[4])

# 3. 判断Oracle表名的字符数并创建stag中间表,删除Oracle数据
v_orastag_tab = str(v_oratab)[:26] + 'stag'
create_stagtab = 'create table %s as select * from %s where 1>2' % (v_orastag_tab.lower(), v_oratab)
delete_sql = ''
find_stagtab = """select * from user_tables where lower(table_name)='%s'""" % v_orastag_tab.lower()
drop_stagtab = 'drop table %s' % v_orastag_tab.lower()
if is_hivetab_partitioned and v_date is not None:
    delete_sql = """delete from %s where logtime=to_date('%s','YYYY-MM-DD')""" % (v_oratab, v_date)
elif is_hivetab_partitioned and v_date is None:
    log_util.log_error('%s.%s is partitioned table,must be specified (-d date)' % (v_hivedb, v_hivetab))
    log_util.log_info('logfile is: %s' % str(logfile))
    sys.exit(1)
else:
    delete_sql = 'truncate table %s' % v_oratab

try:
    conn1 = cx.connect(cx_ora_link)
    cursor1 = conn1.cursor()
    cursor1.execute(find_stagtab)
    result1 = cursor1.fetchall()
    if len(result1) > 0:
        log_util.log_error('oracle stagtable %s already existed,please check !!!' % v_orastag_tab)
        log_util.log_info('logfile is: %s' % str(logfile))
        sys.exit(1)
    else:
        log_util.log_info('create oracle stagtable %s' % v_orastag_tab)
        cursor1.execute(create_stagtab)
    log_util.log_info('delete oracle target table: %s' % delete_sql)
    cursor1.execute(delete_sql)
    conn1.commit()
except Exception, e:
    log_util.log_error('Oracle ERROR: ')
    log_util.log_error(e)
    log_util.log_info('logfile is: %s' % str(logfile))
    sys.exit(1)
# finally:
#     cursor1.close()
#     conn1.close()

# 4. 拼接sqoop export 命令
command_str = ('sqoop export -D mapreduce.map.memory.mb=4096 -D mapreduce.map.java.opts=-Xmx3072m' + ' \\' + '\n'
               + '-D mapred.child.java.opts="-Djava.security.egd=file:/dev/../dev/urandom"' + ' \\' + '\n'
               + '--connect ' + jdbc_link + ' \\' + '\n'
               + '--table ' + str(v_oratab).upper() + ' \\' + '\n'
               + '--hcatalog-database ' + v_hivedb + ' \\' + '\n'
               + '--hcatalog-table ' + v_hivetab + ' \\' + '\n'
               + '--staging-table ' + str(v_orastag_tab).upper() + ' \\' + '\n'
               + '--clear-staging-table' + ' \\' + '\n'
               + "--fields-terminated-by '" + fields_terminat + "' \\" + '\n'
               + """--input-null-string '\\\N' --input-null-non-string '\\\N'""" + ' \\' + '\n'
               + '--validate --outdir '+ logpath + ' \\' + '\n'
               + '--m '+str(v_map))
sqoop_export_command = command_str

if is_hivetab_partitioned:
    sqoop_export_command = command_str + ' \\' + '\n' + '--hcatalog-partition-keys par_dt' + ' \\' + '\n' \
                           + '--hcatalog-partition-values ' + str(v_date)

log_util.log_info('sqoop_export_command: \n' + sqoop_export_command)

# 5. 执行sqoop命令
r_code = 0
try:
    p = subprocess.Popen(sqoop_export_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                         close_fds=True)
    for line in iter(p.stdout.readline, b''):
        line = line.rstrip().decode('utf-8', "ignore")
        line = re.sub('\d+/\d+/\d+ \d+:\d+:\d+', '', line)
        if 'ERROR' in line or 'FAILED' in line:
            log_util.log_error(line)
            r_code = 1
        else:
            log_util.log_info(line)
except Exception, e:
    r_code = 1
    log_util.log_error('Sqoop Execute ERROR !!!')
    log_util.log_error(e)
    log_util.log_info('logfile is: %s' % str(logfile))
    sys.exit(1)
finally:
    p.stdout.close()
    p.wait()

log_util.log_info('sqoop return code :' + str(r_code))
# 6. 删除中间表
log_util.log_info('drop oracle stagtable %s' % v_orastag_tab)
cursor1.execute(drop_stagtab)
cursor1.close()
conn1.close()
log_util.log_info('logfile is: %s' % str(logfile))
log_util.log_info('********** Task End **********')