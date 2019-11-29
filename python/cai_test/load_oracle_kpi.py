#!/usr/bin/python
# -*- coding: UTF-8 -*-

from optparse import OptionParser
import os
import subprocess
import cx_Oracle
import sys
import commands
from ..cai_python.getconn_util import get_conn

parser = OptionParser()
parser.add_option("-g", "--game", dest = "game", help = "game ID")
parser.add_option("-d", "--date", dest = "date", help = "data date")
parser.add_option("-t", "--type", dest = "type", help = "是否排水指标")
parser.add_option("-k", "--key", dest = "key", help = "指标编码")
(options, args) = parser.parse_args()
v_game = options.game
v_date = options.date
v_key = options.key
v_type = options.type

v_datapath='/mnt/data0/logs/dataware_house/to_oracle_data/clientgame'
log_path='/mnt/data0/logs/dataware_house/app/clientgame'
hive_data = ''
v_table = ''
v_ctlfile = ''
v_sqllog = ''
v_badfile = ''
def load_oracle(gameid,keyid,date,*type):
        "hhhhh"
        if len(type) > 0:
                print "this is paishui"
                hive_data = v_datapath+'/kpidata_'+v_game+'_'+v_date+'_'+v_key+'_'+str(type[0])+'.data'
                v_table = 't_clientgame_key_lib_abn'
                v_ctlfile = '/scripts/dataware_house/program/config_file/t_clientgame_key_lib_abn.ctl'
                v_sqllog = log_path+'dykpi_g'+v_game+'d_'+v_date+'_k'+v_key+'_'+str(type[0])+'.sqllog'
                v_badfile = v_datapath+'/kpidata_'+v_game+'_'+v_date+'_'+v_key+'_'+str(type[0])+'.bad'
        else:
                print "not paishui"
                hive_data = v_datapath+'/kpidata_'+v_game+'_'+v_date+'_'+v_key+'.data'
                v_table = 't_clientgame_key_lib'
                v_ctlfile = '/scripts/dataware_house/program/config_file/t_clientgame_key_lib.ctl'
                v_sqllog = log_path+'dykpi_g'+v_game+'d_'+v_date+'_k'+v_key+'.sqllog'
                v_badfile = v_datapath+'/kpidata_'+v_game+'_'+v_date+'_'+v_key+'.bad'

        # 将kpi数据导出文件
        if os.access(hive_data,os.F_OK):
                 os.remove(hive_data)
        command = ('hdfs dfs -getmerge hdfs://CDH-cluster-main/user/hive/warehouse/bi_kpi.db/'+v_table
        +'/par_dt='+v_date+'/par_game='+v_game+'/par_key='+v_key+'/\* '+hive_data)
        print 'hdfs command is ',command
        # os.system(command)
        (status, output) = commands.getstatusoutput(command)
        if status != 0:
                print 'hdfs exitcode: ', status
                print 'HDFS ERROR:', output
                sys.exit(1)

        print 'DataFile is :', hive_data
        data_time = os.popen("cat "+hive_data+"|tail -1|awk -F '\001' '{print $1}'").read().replace('\n','').strip()
        print 'datatime is :', data_time
        ora_data_time = "to_date("+repr(data_time)+",'yyyy-mm-dd')"
        print 'ora_data_time is :', ora_data_time
        del_sql = "delete from "+v_table+" where logtime="+ora_data_time+" and gameid="+v_game+" and keyid="+v_key
        print 'Delete SQL is :', del_sql
        # 删除Oracle数据

        connlink = get_conn('hive').cxoracle_link()
        db_con=cx_Oracle.connect(connlink)
        cursor = db_con.cursor()
        try:
                cursor.execute(del_sql)
                db_con.commit()
                print 'delete oracle kpidata sussusefully'
        except Exception, e:
                db_con.rollback()
                print 'oracle ERROR: ', e
                sys.exit(1)
        finally:
                cursor.close()
                db_con.close()

        # 使用sqlldr导入
        sqlldr_command = ('/opt/app/oracle/11.2.0/bin/sqlldr '+ connlink +' control='
        +v_ctlfile+' data='+hive_data+' log='+v_sqllog+' bad='+v_badfile)
        exec_sqlldr = subprocess.Popen(sqlldr_command,shell=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
        exec_sqlldr.wait()
        sqlldr_output = exec_sqlldr.stdout.read()
        print sqlldr_output
        exec_sqlldr.stdout.close()
        print 'return code is :', exec_sqlldr.returncode

if __name__ == "__main__":
        load_oracle(v_game,v_key,v_date)