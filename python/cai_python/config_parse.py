#!/usr/bin/python
# -*- coding: UTF-8 -*-
import ConfigParser


class GetClusterConn():
    """

    """
    def __init__(self,dbname):
        self.dbname = dbname
        config = ConfigParser.RawConfigParser()
        conf_file = 'E:\work_files\cluster_conn.ini'
        config.read(conf_file)
        self.username = str(config.get(self.dbname, 'user'))
        self.password = str(config.get(self.dbname, 'password'))
        self.port = int(config.get(self.dbname, 'port'))
        self.host = str(config.get(self.dbname, 'host'))

    def getconn_config(self):
        __conn_dict = {}
        __conn_dict['host'] = self.host
        __conn_dict['port'] = self.port
        __conn_dict['user'] = self.username
        __conn_dict['password'] = self.password
        return __conn_dict

if __name__ == '__main__':
    getconn = GetClusterConn('hive-jdbc')
    print getconn.getconn_config()