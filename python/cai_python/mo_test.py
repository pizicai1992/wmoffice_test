#!/usr/bin/python
# -*- coding: UTF-8 -*-

import getconn_util
from logutil import LogUtil

# get_con = getconn_util.get_conn('hive')
# print 'jdbc : '+get_con.jdbc_link()
# print 'cxoracle : '+get_con.cxoracle_link()
# print 'orauser: '+get_con.ora_user()
logfile = r'E:/work_files/logutil_test.log'
log = LogUtil(logfile,level='warn')
log.info('***** test info ******')
log.info('***** test sads ******')
log.info('***** test fdfdf ******')
log.warn('***** test sdad ******')
log.error('***** test error ******')