#!/usr/bin/python
#encoding:utf-8

import cx_Oracle as cx
import sys,os
from ..cai_python.getconn_util import get_conn
reload(sys)
sys.setdefaultencoding("utf-8")
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

key_comment='当前设备数'
connlink = get_conn('hive').cxoracle_link()
ora_conn = connlink
db_conn = cx.connect(ora_conn)
cursor = db_conn.cursor()
sql2 = "select COLUMN_NAME,column_type from t_dic_hivedw_columnname where column_comment = :key_comment"
cursor.prepare(sql2)
cursor.execute(None, {'key_comment': key_comment})
keyword = cursor.fetchall()
print keyword
print ' '.join(keyword[0])