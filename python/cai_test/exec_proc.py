#!/usr/bin/python
#encoding:utf-8

import cx_Oracle as cx
from optparse import OptionParser

#声明变量
parser = OptionParser()
parser.add_option("-p", "--proc", dest = "proc", help = "proc name")
parser.add_option("-g", "--game", dest = "game", help = "game ID")
parser.add_option("-d", "--date", dest = "date", help = "data date")
(options, args) = parser.parse_args()
proc = options.proc
gameId = options.game
dataDate = options.date
conn = cx.connect("hive/hive_2017@//10.14.250.17:1521/dfdb")
cursor = conn.cursor()

cursor.prepare("select count(*) from USER_ARGUMENTS where lower(OBJECT_NAME) = :proc and IN_OUT='OUT'")
cursor.execute(None,{'proc':proc})
result = cursor.fetchone()
# print type(result)
print '输出参数个数: ',result[0]
# plsql出参
print 'gameid: ',gameId,' date: ',dataDate
rnum = cursor.var(cx.NUMBER)
msg = cursor.var(cx.STRING)
if result[0] > 0:
    cursor.callproc(proc, [dataDate, gameId, rnum, msg])
    print rnum.getvalue()
    print msg.getvalue()
else:
    cursor.callproc(proc, [dataDate, gameId])

#资源关闭
cursor.close()
conn.close()
