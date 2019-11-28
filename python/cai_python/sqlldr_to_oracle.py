#!/usr/bin/python
# -*- coding: UTF-8 -*-

import sys, getopt, os
sys.path.append("/scripts/dataware_house/program/common")
import PyCommonFunc
import cx_Oracle as cx
import subprocess
import argparse

def get_oracle_link(oracle_base):
    (rs, op, dest_db_info) = PyCommonFunc.get_dest_db_info(oracle_base)
    cxoracle_link = '%s/%s@%s:%s/%s' % \
                  (dest_db_info[0], dest_db_info[1], dest_db_info[2], dest_db_info[3], dest_db_info[4])
    oracle_user = dest_db_info[0]
    sqlldr_link = '%s/%s@%s/%s' % \
                  (dest_db_info[0], dest_db_info[1], dest_db_info[2], dest_db_info[4])
    return cxoracle_link, sqlldr_link, oracle_user


def get_ctlfile(oracle_link, table_name, separator='\t'):
    """生成sqlldr控制文件
    :parameter:
        oracle_link: oracle的连接串
        table_name: Oracle的表名
    :return: sqlldr CTL filename
    """
    ctlfile = os.path.join('/var/lib/impala/test/cai_file/myscript',str(table_name)+'.ctl')
    try:
        conn = cx.connect(oracle_link)
        cursor = conn.cursor()
        sqlstr = """
        select column_name || ' ' || decode(data_type,
                                    'NUMBER',
                                    'INTEGER EXTERNAL',
                                    'VARCHAR2',
                                    'CHAR(4000)',
                                    'DATE',
                                    'date "yyyy/mm/dd hh24:mi:ss"',
                                    '') || ' ' || ' "replace(:' ||
       column_name || ',''NULL'',0)",' clname

          from user_tab_columns
         where lower(table_name) = :tab_name
         order by column_id
        """
        cursor.prepare(sqlstr)
        cursor.execute(None, {'tab_name': str(table_name).lower()})
        sql_result = cursor.fetchall()
        with open(ctlfile,'w') as f:
            f.write('load data characterset utf8'+'\n')
            f.write('append into table ' + str(table_name).lower() + '\n')
            f.write('fields terminated by ' + repr(separator) + '\n')
            f.write('trailing nullcols' + '\n')
            f.write('('+'\n')
            for i in sql_result:
                if i == sql_result[-1]:
                    recode = i[0][:-1]
                else:
                    recode = i[0]
                # print recode
                f.write(recode+'\n')
            f.write(')'+'\n')
    except Exception, e:
        print e
    finally:
        cursor.close()
        conn.close()
    print '======================== ctl ====================='
    with open(ctlfile, 'r') as f:
        print f.read()
    return ctlfile


def run_sqlldr(sqlldr_link, ctlfile, datafile):
    r_code = 0
    cmd = 'sqlldr userid=' + sqlldr_link + ' data=' + datafile + ' control=' + \
        ctlfile + ' errors=1 direct=true'
    print '======================== sqlldr ====================='
    print cmd
    try:
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, close_fds=True)
        for line in iter(p.stdout.readline, ''):
            line = line.rstrip().decode('utf-8', "ignore")
            print line
            if 'ERROR' in line or 'FAILED' in line:
                r_code = 1
            else:
                pass
    except Exception, e:
        r_code = 1
        print 'Sqoop Execute ERROR !!!'
        sys.exit(1)
    finally:
        p.stdout.close()
        p.wait()
    print 'Return Code of SQLldr Execute :', str(r_code)
    return r_code


def main():
    parser = argparse.ArgumentParser(description="Syn data to oracle table by SQLldr")
    parser.add_argument('-b', '--base', help='Oracle连接标识')
    parser.add_argument('-t', '--table', help='Oracle表名')
    parser.add_argument('-f', '--file', help='数据文件')
    args = parser.parse_args()
    oradb = args.base
    oratab = args.table
    datafile = args.file
    cxoracle_link, sqlldr_link, oracle_user = get_oracle_link(oradb)
    ctl = get_ctlfile(cxoracle_link, oratab)
    run_sqlldr(sqlldr_link, ctl, datafile)

if __name__ == '__main__':
    main()