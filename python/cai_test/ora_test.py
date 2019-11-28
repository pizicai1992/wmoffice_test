#!/usr/bin/python
#encoding:utf-8

import cx_Oracle as cx
from optparse import OptionParser

column = ()
conn = cx.connect("bitask/viewsonic2010@10.14.250.9:1521/wmdw")
cursor = conn.cursor()
tab_name = 't_dw_dota2_glog_dotalink'
cursor.prepare("select lower(column_name)||'=string' from user_tab_columns where lower(table_name) = :tab_name order by column_id")
cursor.execute(None,{'tab_name':tab_name})
result = cursor.fetchall()
print result
print type(result)
#print ','.join(result)
for c in result:
    column = column+c
cursor.close()
conn.close()
print column
print 'lala:',','.join(column)
print "sqoop "+column.__str__()