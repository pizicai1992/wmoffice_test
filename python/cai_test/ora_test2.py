#!/usr/bin/python
#encoding:utf-8

import cx_Oracle as cx
import sys,os
reload(sys)
sys.setdefaultencoding("utf-8")
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

key_comment='当前设备数'
ora_conn = 'hive/hive_2017@10.14.250.17:1521/dfdb'
db_conn = cx.connect(ora_conn)
cursor = db_conn.cursor()
sql2 = "select COLUMN_NAME,column_type from t_dic_hivedw_columnname where column_comment = :key_comment"
cursor.prepare(sql2)
cursor.execute(None, {'key_comment': key_comment})
keyword = cursor.fetchall()
print keyword
print ' '.join(keyword[0])