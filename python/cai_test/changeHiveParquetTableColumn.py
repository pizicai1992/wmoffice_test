#!/usr/bin/python
# -*- coding: UTF-8 -*-

from hive_util2 import HiveUtil
import argparse
import subprocess
import re
import sys


def execute_hive(sql):
    hive = HiveUtil()
    hive.execute_sql(sql)


def exec_cmd(cmd):
    """执行shell"""
    r_code = 0
    # log = log_utils()
    try:
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT, close_fds=True)
        for line in iter(p.stdout.readline, ''):
            line = line.rstrip().decode('utf-8', "ignore")
            # line = re.sub('\d+/\d+/\d+ \d+:\d+:\d+', '', line)
            if 'ERROR' in line or 'FAILED' in line:
                print line
                r_code = 1
            else:
                print line
    except Exception, e:
        r_code = 1
        print 'Execute ERROR !!!'
        print e
        sys.exit(1)
    finally:
        p.stdout.close()
        p.wait()
    # print 'Return Code of Sqoop Execute :',str(r_code)
    return r_code


def main():
    parser = argparse.ArgumentParser(
        description="change parquet table column", conflict_handler='resolve')
    parser.add_argument('-t', '--tablename', required=True,
                        help='example: bi_tmp.t_test_tab')
    parser.add_argument('-p', '--previous_column', required=True,
                        help='改变之前的列名, example=userid')
    parser.add_argument('-c', '--current_column',
                        required=True, help='改变之后的表名, example=userid')
    parser.add_argument('-e', '--column_type', required=True,
                        help='改变之后的列类型, example=string')
    args = parser.parse_args()
    hive_table = args.tablename
    p_column = args.previous_column
    c_column = args.current_column
    column_type = args.column_type
    db_name, tb_name = hive_table.split('.')
    # 1. 在bi_tmp库创建一个外部表，表结构跟原始的目标表结构一样，指定location 为原始表的位置
    create_ext_table = 'create EXTERNAL table bi_tmp.%s_ext like %s location "hdfs://CDH-cluster-main/user/hive/warehouse/%s.db/%s"' % (
        tb_name, hive_table, db_name, tb_name)
    execute_hive(create_ext_table)
    # 2. 目标表修改字段名或字段类型
    alter_column = 'alter table %s change %s %s %s CASCADE' % (
        hive_table, p_column, c_column, column_type)
    execute_hive(alter_column)
    # 3. 修复新创建的外部表的分区
    msck_partition = 'MSCK REPAIR TABLE bi_tmp.%s_ext' % tb_name
    execute_hive(msck_partition)
    # 4. 使用新创建的外部表查询数据然后动态分区插入到 修改字段后的目标表中
    insert_sql = 'insert overwrite table %s partition(par_dt) select * from bi_tmp.%s_ext;' % (
        hive_table, tb_name)
    insert_data = 'hive -v -i /var/lib/impala/test/dongtai.conf -e "%s"' % insert_sql
    exec_cmd(insert_data)
    # 5. 删掉创建的tmp库下的外部表
    drop_ext_table = 'drop table bi_tmp.%s_ext' % tb_name
    execute_hive(drop_ext_table)


if __name__ == '__main__':
    main()
