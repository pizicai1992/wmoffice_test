#!/usr/bin/python
# -*- coding: UTF-8 -*-

import ConfigParser

config = ConfigParser.RawConfigParser()
conf_file = 'E:\datawarehouse\datawarehouse\cfg\oracle_connect.config'
config.read(conf_file)
ora_username = str(config.get('new_db','username'))
ora_password = str(config.get('new_db','password'))
ora_ip = str(config.get('new_db','ip'))
ora_host = str(config.get('new_db','host'))
ora_servname = str(config.get('new_db','service_name'))
print 'jdbclink : jdbc:oracle:thin:@%s:%s/%s -username %s -pasword %s' % \
      (ora_ip,ora_host,ora_servname,ora_username,ora_password)
print 'cxOralink : %s/%s@%s:%s/%s' % (ora_username,ora_password,ora_ip,ora_host,ora_servname)