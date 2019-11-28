#!/usr/bin/python
# -*- coding: UTF-8 -*-

# from pyhive import hive
#
# conn = hive.connect(host='10.14.252.26', port=10001, username='impala',auth='NONE')
# cursor = conn.cursor()
# sql='show tables'
# cursor.execute(sql)
# res = cursor.fetchall()
# for i in res:
#     print i
#
# cursor.close()
# conn.close()

from impala.dbapi import connect

conn = connect(host='10.14.252.26', port=10001, auth_mechanism='PLAIN')
cur = conn.cursor()

cur.execute('SHOW DATABASES')
print(cur.fetchall())

cur.execute('SHOW Tables')
print(cur.fetchall())