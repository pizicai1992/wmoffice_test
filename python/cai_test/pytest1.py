#!/usr/bin/python
# -*- coding: UTF-8 -*-
import datetime
import sys, getopt
from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext
from pyspark.sql import Row

if len(sys.argv[1:]) < 1:
    print 'ERROR : 参数不能为空'
    sys.exit(1)
opts, args = getopt.getopt(sys.argv[1:], '-h-g:-k:-d:-f:', ['help', 'gameid=', 'keyid=', 'date=', 'file='])
try:
    for opt_name, opt_value in opts:
        if opt_name in ('-h', '-help'):
            print 'Usage: 执行示例:python dy_run_kpi.py -g 4 -k 100044 -d 2018-01-01'
            sys.exit(1)
        if opt_name in ('-g', '-gameid'):
            global v_game
            global v_table
            v_game = opt_value
            v_table = '%06d' % int(v_game)
        if opt_name in ('-k', '-keyid'):
            global v_keyid
            v_keyid = opt_value
        if opt_name in ('-d', '-date'):
            global v_date
            v_date = opt_value
        if opt_name in ('-f', '-file'):
            global v_file
            v_file = opt_value
except getopt.GetoptError:
    print("ERROR : 参数错误");
    print 'Usage: 请输入 -g 4 -k 100044 -d 2018-01-01';
    sys.exit(1);
startTime = datetime.datetime.now()
taskstarttime = startTime.strftime("%Y-%m-%d %H:%M:%S")
conf = SparkConf().setAppName('test_pyspark')
sc = SparkContext(conf=conf)
sc.setLogLevel('WARN')
sqlContext = HiveContext(sc)
exec_sql = ''
with open(v_file) as fsql:
    exec_sql = fsql.read().replace('${hivevar:vdate}', v_date).replace('${hivevar:vgame}', v_game).replace(
        '${hivevar:vtable}', v_table)

print exec_sql
reql = exec_sql.strip('\n').split(';')
print repr(reql)
for sql in reql:
    if len(sql) > 1:
        print 'exec sql is :', sql
    sqlContext.sql(sql.strip())
pyendtime = datetime.datetime.now()
print 'pyspark exec times is :' + str((pyendtime - startTime).seconds) + ' seconds'
databs = sqlContext.sql("select * from default.test_lib2 where par_dt='%s' and par_game=%d and par_key=%d"
                        % (v_date, int(v_game), int(v_keyid)))
fl = open('list.txt', 'w')
for i in databs:
    print '|'.join(
        [i['logtime'], i['proctimes'], str(i['gameid']), str(i['groupname']), str(i['keyid']), str(i['keyvalue'])])
# fl.write('|'.join([i['logtime'],i['proctimes'],str(i['gameid']),str(i['groupname']),str(i['keyid']),str(i['keyvalue'])]))
#  fl.write("\n")

fl.close()
taskendtime = datetime.datetime.now()
print 'Task Run Time :' + str((taskendtime - startTime).seconds) + ' seconds'