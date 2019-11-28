#!/usr/bin/python
# -*- coding: UTF-8 -*-
import sys
reload(sys)
sys.setdefaultencoding( "utf-8" )

from kafka import KafkaProducer
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import time

def main():
    ##生产模块
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
    with open('/var/lib/impala/shell/cai_test/in_file/zx_charge_20180407/000000_0', 'r') as f:
        for line in f.readlines():
            time.sleep(2)
            producer.send("test_cai", line.replace("\n", ""))
            print line
            # producer.flush()


if __name__ == '__main__':
    main()