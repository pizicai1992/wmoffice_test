#!/usr/bin/python
# -*- coding: UTF-8 -*-

import sys,getopt
import logging
import os
import commands
import datetime

startTime = datetime.datetime.now()
taskstarttime = startTime.strftime("%Y-%m-%d %H:%M:%S")
if len(sys.argv[1:]) <1:
        print 'ERROR : 参数不能为空'
        sys.exit(1)
opts,args = getopt.getopt(sys.argv[1:],'-h-g:-k:-d:',['help','gameid=','keyid=','date='])
try:
        for opt_name,opt_value in opts:
                if opt_name in ('-h','-help'):
                        print 'Usage: 执行示例: '
                        sys.exit(1)
                if opt_name in ('-g','-gameid'):
                        global v_game
                        global v_table
                        v_game=opt_value
                        v_table='%06d' % int(v_game)
                        print '游戏ID:',v_game,
                        print '表名:',v_table,
                if opt_name in ('-k','-keyid'):
                        global v_keyid
                        v_keyid=opt_value
                        print '指标ID:',v_keyid,
                if opt_name in ('-d','-date'):
                        global v_date
                        v_date=opt_value
                        print '执行日期:',v_date
except getopt.GetoptError:
    print("ERROR : 参数错误");
    print 'Usage: 请输入';
    sys.exit(1);
log_path='/var/lib/impala'
sqlfile_path='/scripts/dataware_house/script/app/common/clientgame'
log_file=log_path+'/clientgamekpi_'+v_game+'_'+v_date+'_'+v_keyid+'.log'
sql_file=sqlfile_path+'/clientgame_kpi_'+v_keyid+'_d.sql'

if not os.access(sql_file,os.F_OK):
        print 'SQL文件不存在:',sql_file
        sys.exit(1)
if os.access(log_file,os.F_OK):
        os.remove(log_file)

logger = logging.getLogger('mylogger')
logger.setLevel(logging.INFO)
# 创建一个handler，用于写入日志文件
fh = logging.FileHandler(log_file)
fh.setLevel(logging.INFO)
# 再创建一个handler，用于输出到控制台
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
# 定义handler的输出格式
formatter = logging.Formatter('%(asctime)s - %(levelname)s :%(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)
# 给logger添加handler
logger.addHandler(fh)
logger.addHandler(ch)
# 记录一条日志
logger.info('Task start ...........')
command = ('hive --hiveconf mapred.job.name=game:'+v_game+'-kpi:'+v_keyid+'-date:'+v_date+' \\'+'\n'
+'  --hiveconf spark.app.name=game'+v_game+'__kpi'+v_keyid+'__date'+v_date+' \\'+'\n'
+'  -f '+'\''+sql_file+'\''+' \\'+'\n'
+'  -d vdate='+v_date+' -d vgame='+v_game+' -d vtable='+v_table+' -d vkpitype='+v_keyid+' -v ')
logger.info('执行命令 : '+'\n'+command)
#(status, output) = commands.getstatusoutput(command)
os.system(command+'2>&1 |tee -a '+'\''+log_file+'\'')
endTime = datetime.datetime.now()
taskendtime = endTime.strftime("%Y-%m-%d %H:%M:%S")
#logger.info(output)
#logger.info('命令运行状态'+str(status))
fileHandler = open(log_file)
for line in fileHandler.readlines():
        if 'FAILED' in line:
                        logger.error(line)
                        logger.info('运行时间 :'+str((endTime-startTime).seconds)+'秒')
                        sys.exit(1)
fileHandler.close()
logger.info('运行时间 :'+str((endTime-startTime).seconds))
