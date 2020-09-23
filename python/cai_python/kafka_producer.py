#!/usr/bin/python
# -*- coding: UTF-8 -*-

import time
from kafka import KafkaProducer as k_pro

producer = k_pro(bootstrap_servers=['192.168.10.10:9092'], key_serializer= str.encode, value_serializer= str.encode)
msg_count = 0
for line in open('/home/hadoop/myfile/hadoop-3.2.1/LICENSE.txt'):
    msg = line.rstrip('\n')
    producer.send('testcai', msg)
    msg_count += 1
    time.sleep(1)
    if msg_count == 200:
        break

producer.close()