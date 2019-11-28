#!/usr/bin/python
# -*- coding: UTF-8 -*-

from pyhive import hive

conn = hive.Connection(host='10.14.240.254', port=10182, username="impala",password='impala_+-', auth='LDAP')
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