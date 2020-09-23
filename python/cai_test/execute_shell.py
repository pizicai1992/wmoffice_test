#!/usr/bin/python
# -*- coding: UTF-8 -*-

import subprocess
import re
import sys
sys.path.append("D:\\datawarehouse\\program\\common")
import PyCommonFunc


class ExecShellCmd():
    def __init__(self):
        pass

    def exec_cmd(self, cmd):
        r_code = 0
        # log = log_utils()
        try:
            p = subprocess.Popen(
                cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, close_fds=True)
            for line in iter(p.stdout.readline, ''):
                line = line.rstrip().decode('utf-8', "ignore")
                # line = re.sub('\d+/\d+/\d+ \d+:\d+:\d+','',line)
                if 'ERROR' in line or 'FAILED' in line:
                    print line
                    r_code = 1
                else:
                    print line
        except Exception, e:
            r_code = 1
            print 'ERROR %s' % e
            sys.exit(1)
        finally:
            p.stdout.close()
            p.wait()
        return r_code

    def exec_cmd_withlog(self, cmd, logfile):
        log = PyCommonFunc.LogUtil(logfile)
        r_code = 0
        try:
            p = subprocess.Popen(
                cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, close_fds=True)
            for line in iter(p.stdout.readline, ''):
                line = line.rstrip().decode('utf-8', "ignore")
                line = re.sub(r'\d+/\d+/\d+ \d+:\d+:\d+', '', line)
                if 'ERROR' in line or 'FAILED' in line:
                    log.error(line)
                    r_code = 1
                else:
                    log.info(line)
        except Exception, e:
            r_code = 1
            log.error('Execute ERROR !!!')
            log.error(e)
            sys.exit(1)
        finally:
            p.stdout.close()
            p.wait()
        # print 'Return Code of Sqoop Execute :',str(r_code)
        return r_code
