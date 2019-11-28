#!/usr/bin/python
# -*- coding: UTF-8 -*-

# subprocess模块使用

import subprocess

sbp = subprocess.Popen('hive -e "show databasees"', shell=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
#sbp_result = sbp.stdout.readlines()
#for db in sbp_result:
#    print '***** databases *****'
#    print db.decode('utf-8')
#sbp.stdout.close()
sbp.wait()
sbp_re2 = sbp.stdout.read()
print sbp_re2
sbp.stdout.close()
print 'return code is :', sbp.returncode