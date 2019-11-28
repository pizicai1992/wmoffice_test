#!/usr/bin/python
# -*- coding: UTF-8 -*-
# 解析XML文件
from xml.dom.minidom import parse
import xml.dom.minidom

DomTree = xml.dom.minidom.parse('E:/datawarehouse/program/config_file/ora_conf.xml')
collection = DomTree.documentElement
databases = collection.getElementsByTagName('database')
username = ''
password = ''
ip_port = ''
sid = ''
link = ''
def print_oarcle_link(dbname):
    for database in databases:
        name = database.getElementsByTagName('name')[0].childNodes[0].data
        if name == dbname:
            username = database.getElementsByTagName('username')[0].childNodes[0].data
            password = database.getElementsByTagName('password')[0].childNodes[0].data
            ip_port = database.getElementsByTagName('ip_port')[0].childNodes[0].data
            sid = database.getElementsByTagName('sid')[0].childNodes[0].data
            link = username+'/'+password+'@'+ip_port+'/'+sid
    return link

def print_oracle_jdbc_sqoop(dbname):
    for database in databases:
        name = database.getElementsByTagName('name')[0].childNodes[0].data
        if name == dbname:
            username = database.getElementsByTagName('username')[0].childNodes[0].data
            password = database.getElementsByTagName('password')[0].childNodes[0].data
            ip_port = database.getElementsByTagName('ip_port')[0].childNodes[0].data
            sid = database.getElementsByTagName('sid')[0].childNodes[0].data
            link = 'jdbc:oracle:thin:@'+ip_port+'/'+sid+' -username '+username+' -password '+password
    return link

if __name__ == "__main__":
    ora_link = print_oarcle_link('hive')
    sqoop_link = print_oracle_jdbc_sqoop('newrac')

    print 'oracle_link : ', ora_link
    print 'sqoop_link :', sqoop_link

