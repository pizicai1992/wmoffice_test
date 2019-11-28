#!/usr/bin/python
# -*- coding: UTF-8 -*-
from kafka import KafkaProducer
import time
import random

producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

for i in range(50):
    msg = ''.join(random.sample('zyxwvutsrqponmlkjihgfedcba',5))
    producer.send('testcai_1', bytes(msg+' 1'))
    print msg
    time.sleep(1)
