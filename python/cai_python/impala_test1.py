#!/usr/bin/python
# -*- coding: UTF-8 -*-

from impala.dbapi import connect
from impala.util import *
import csv
import argparse
from config_parse import GetClusterConn as getconn

conn_dict = getconn('hive-jdbc').getconn_config()

class ImpalaUtil():
    def __init__(self, host, port, user, password, auth_mechanism='LDAP'):
        self.conn = connect(auth_mechanism=auth_mechanism, **conn_dict)
        self.cursor = self.conn.cursor()

    def _close(self):
        self.cursor.close()
        self.conn.close()

    def execute_sql(self, sql_string):
        self.cursor.execute(sql_string)
        columns = []
        if self.cursor.description:
            columns = [data[0] for data in self.cursor.description]
        sql_result = self.cursor.fetchall()
        self._close()
        return columns, sql_result

    def export_csv(self, sql_string, data_file, print_header=False):
        self.cursor.execute(sql_string)
        column_name = [data[0] for data in self.cursor.description]
        # column_name, sql_data = self.execute_sql(sql_string)
        with open(data_file, 'w') as outcsv:
            writer = csv.writer(outcsv, delimiter=',', quotechar='"',  # quoting=csv.QUOTE_ALL,
                                lineterminator='\n')
            if print_header:
                writer.writerow(column_name)
            for row in self.cursor:
                writer.writerow(row)

        self._close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="python连接impala的工具类")
    parser.add_argument('-s', '--sql', help='SQL语句')
    parser.add_argument('-f', '--file', help='数据文件')
    parser.add_argument('-p', '--printhead', help='打印列名')
    args = parser.parse_args()
    sql_str = args.sql
    datafile = args.file
    print_header = args.printhead
    imp = ImpalaUtil()
    # imp.export_csv(sql_string=sql_str, data_file=datafile, print_header=print_header)
    imp.execute_sql(sql_str)
