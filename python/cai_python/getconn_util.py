#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
获取Oracle的连接串
"""
import logging
from xml.dom.minidom import parse
import xml.dom.minidom
from optparse import OptionParser

import os


class get_conn():
    def __init__(self,dbname):
        self.ora_conf = 'E:/datawarehouse/datawarehouse/cfg/oracle_connect.config'
       # self.ora_conf = '/scripts/dataware_house/program/config_file/ora_conf.xml'
        self.domtree = xml.dom.minidom.parse(self.ora_conf)
        self.databases = self.domtree.documentElement.getElementsByTagName('database')
        self.dbname = dbname
        for database in self.databases:
            name = database.getElementsByTagName('name')[0].childNodes[0].data
            if name == self.dbname:
                self.username = database.getElementsByTagName('username')[0].childNodes[0].data
                self.password = database.getElementsByTagName('password')[0].childNodes[0].data
                self.ip_port = database.getElementsByTagName('ip_port')[0].childNodes[0].data
                self.sid = database.getElementsByTagName('sid')[0].childNodes[0].data
    def jdbc_link(self):
        jdbclink = ''
        jdbclink = 'jdbc:oracle:thin:@' + self.ip_port + '/' + self.sid + ' -username ' + self.username + ' -password ' + self.password
        return jdbclink

    def cxoracle_link(self):
        oralink = self.username + '/' + self.password + '@' + self.ip_port + '/' + self.sid
        return oralink
    def ora_user(self):
        orauser = self.username
        return orauser

class log_util():
    def __init__(self, log_name):
        self.log_path = 'E://work_files//'
        self.log_file = self.log_path + log_name
        # self.log_mess = str(log_message)
        if os.access(self.log_file, os.F_OK):
            os.remove(self.log_file)
        self.logger = logging.getLogger('sqoop_syn_dictable_logger')
        self.logger.setLevel(logging.INFO)
        self.fh = logging.FileHandler(self.log_file)
        self.fh.setLevel(logging.INFO)
        self.ch = logging.StreamHandler()
        self.ch.setLevel(logging.INFO)
        self.formatter = logging.Formatter('[%(asctime)s - %(levelname)s] : %(message)s')
        self.fh.setFormatter(self.formatter)
        self.ch.setFormatter(self.formatter)
        self.logger.addHandler(self.fh)
        self.logger.addHandler(self.ch)

    def log_info(self, info_mess):
        self.logger.info(info_mess)

    def log_error(self, error_mess):
        self.logger.error(error_mess)

if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option("-b", "--base", dest="base", help="oracle的库标识")
    parser.add_option("-c", "--conn", dest="conn", help="需要得到的连接类型")
    (options, args) = parser.parse_args()
    v_base = options.base
    v_conn = options.conn
    get_con = get_conn(v_base)
    result = ''
    if int(v_conn) == 1:
        result = get_con.jdbc_link()
    elif int(v_conn) == 2:
        result = get_con.cxoracle_link()
    else:
        result = get_con.ora_user()

    print result
