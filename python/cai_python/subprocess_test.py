#!/usr/bin/python
# -*- coding: UTF-8 -*-

import subprocess
from datetime import datetime

def run_comd(command, print_msg=True):
#    p = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,close_fds=True)
    start_time=datetime.now()
    p = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    lines = []
    for line in iter(p.stdout.readline, b''):
        #print chardet.detect(line)
        line = line.rstrip().decode('gbk',"ignore")
        lines.append(line)
        end_time = datetime.now()
        time_duration = (end_time-start_time).seconds
        print 'time used %d seconds' % time_duration
        print line
    p.stdout.close()
    p.wait()
    return lines

def run_cmd2(command):
    p = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    out, err = p.communicate()
    print out.rstrip().decode('gbk',"ignore")

run_comd('ping www.baidu.com')
#run_cmd2('ping www.baidu.com')