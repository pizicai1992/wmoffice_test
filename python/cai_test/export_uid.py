# -*- coding: UTF-8 -*-

import os
import time
import subprocess
import re
import sys
sys.path.append("/scripts/dataware_house/program/common")
from PyCommonFunc import ImpalaUtil
from hive_test2 import hiveUtil
from textwrap import dedent
import argparse

# SQL_FILE = '/var/lib/impala/test/cai_file/myscript/sms_dy_uid.sql'
TODAY = time.strftime('%Y%m%d')

def run_impala(sdate, edate, sqlFile):
    sqlString = ''
    tableTime = TODAY
    tabelName = 'bi_tmp.t_dy_userid_' + TODAY
    with open(sqlFile,'r') as f:
        sqlString = f.read()
    sqlString = sqlString.replace('${table_date}', tableTime)
    sqlString = sqlString.replace('${start_date}', sdate)
    sqlString = sqlString.replace('${end_date}', edate)
    print sqlString
    drop_tab = 'drop table if exists bi_tmp.%s' % 't_dy_userid_'+tableTime
    impala = ImpalaUtil()
    hive = hiveUtil()
    # impala.execute_sql(drop_tab)
    hive.queryHive(drop_tab)
    impala.invalidate_metadata(tabelName)
    impala.execute_sql(sqlString)
    impala._close()

def exec_cmd(cmd):
    """执行shell命令"""
    r_code = 0
    # log = log_utils()
    try:
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,close_fds=True)
        for line in iter(p.stdout.readline, ''):
            line = line.rstrip().decode('utf-8', "ignore")
            # line = re.sub('\d+/\d+/\d+ \d+:\d+:\d+','',line)
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

def parse_arguments():
    parser = argparse.ArgumentParser(description="export clientgame userid info",conflict_handler='resolve')
    parser.add_argument('-s','--startdate', required=True, help="指定流失用户的开始日期, example: 2014-01-01")
    parser.add_argument('-e','--enddate', required=True, help="指定流失用户的结束日期, example: 2019-12-01")
    parser.add_argument('-g','--gamelist', required=True, help="指定游戏列表, example: 2,3,4")
    parser.add_argument('-t','--type', required=True, help="指定导出类型:1导出uid；2导出uid,lev, example: 2")
    args = parser.parse_args()
    startdate = args.startdate
    enddate = args.enddate
    gamelist = args.gamelist
    datatype = args.type
    return (startdate, enddate, gamelist, datatype)

def main():
    sdate, edate, glist, dtype = parse_arguments()
    SQL_FILE = ''
    column_list = ''
    if dtype == 1:
        SQL_FILE = '/var/lib/impala/test/cai_file/myscript/sms_dy_uid.sql'
        column_list = 'userid'
    else:
        SQL_FILE = '/var/lib/impala/test/cai_file/myscript/sms_dy_uid_lev.sql'
        column_list = 'userid,lev'
    run_impala(sdate, edate, SQL_FILE)
    game_dict = {1:'完美世界', 2:'武林外传', 3:'完美国际', 4:'诛仙', 5:'赤壁' ,
             6:'热舞派对', 7:'口袋西游', 8:'神鬼传奇', 9:'梦幻诛仙', 11:'神魔大陆',
             12:'神鬼世界', 15:'笑傲江湖', 17:'圣斗士星矢', 18:'射雕英雄传', 73:'蜀山缥缈录'}
    for gid in glist.split(','):
        gameName = game_dict[int(gid)]
        varDict = {}
        varDict['today'] = TODAY
        varDict['gameid'] = int(gid)
        varDict['gamename'] = gameName
        varDict['column'] = column_list
        export_cmd = dedent("""\
        impala-shell -i 10.14.240.254:25003 -l -u impala --auth_creds_ok_in_clear --ldap_password_cmd="echo -n impala_+-" -q \\
        "select {column} from bi_tmp.t_dy_userid_{today} where gameid={gameid}" \\
        -o /var/lib/impala/test/cai_file/file/sms_userid_20200617/{gamename}.csv -B \\
        --print_header --output_delimiter=','""".format(**varDict))
        print export_cmd
        exec_cmd(export_cmd)

if __name__ =='__main__':
    main()