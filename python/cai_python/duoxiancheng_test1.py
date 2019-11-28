#!/usr/bin/python
# -*- coding: UTF-8 -*-

import threadpool
import time


def print_hello(str):
    print 'hello ', str
    time.sleep(2)


str_list = ['sdada', 'dadfrfrg', 'grthyjj', 'graa', 'tytyu', 'opouii']
start_time = time.time()
pool = threadpool.ThreadPool(3)
requests = threadpool.makeRequests(print_hello, str_list)
[pool.putRequest(req) for req in requests]
pool.wait()
print '%d second' % (time.time() - start_time)
