#!/usr/bin/python
# -*- coding: UTF-8 -*-

from pyhive import hive
from config_parse import GetClusterConn as getconn

conn_dict = getconn('hive-jdbc').getconn_config()
conn = hive.Connection(auth='LDAP', **conn_dict)
query_sql = "select * from bi_dwd.t_dwd_acct_000003_chardata t where par_dt='2019-09-28' limit 30"
curosr = conn.cursor()
curosr.execute("set role admin")
curosr.execute(query_sql)

# 获得列的信息
columns = curosr.description
print columns
# 获取全部数据，result是tuple
for result in curosr.fetchall():
    print(result)

curosr.close()