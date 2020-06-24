#!/usr/bin/python
# -*- coding: UTF-8 -*-

from impala.dbapi import connect
from impala.util import *
import csv
import argparse


class ImpalaUtil():
    """Util class to handle Impala

    a util class to handle Impala by jdbc;

    Attributes:
        conn: connection to impala server
        cursor: cursor to handle query
    """

    def __init__(self, host='tx-dfha-db-vip', port=7999, user='impala', password='impala_+-', auth_mechanism='PLAIN'):
        self.conn = connect(host, port, user=user, password=password, auth_mechanism=auth_mechanism)
        self.cursor = self.conn.cursor()

    def _close(self):
        self.cursor.close()
        self.conn.close()

    def execute_sql(self, sql_string):
        """execute query
        Args:
            sql_string: SQl string to execute,exp: show databases like '*iwm*'
        Returns:
            fetched row data as tuple :([columns], [row data])
            example:
            (['name', 'comment'],[('ods_bmiwm', ''),('ods_rbiwm', '')])
        Raises:
            some Exception
        """
        self.cursor.execute(sql_string)
        columns = []
        sql_result = []
        if self.cursor.description:
            columns = [data[0] for data in self.cursor.description]
        sql_result = self.cursor.fetchall()
        # self._close()
        return columns, sql_result

    def export_csv(self, sql_string, data_file, print_header=False):
        """execute query and export result to CSV file
        Args:
            sql_string: SQl string to execute,exp: show databases like '*iwm*'
            data_file: a file to export row data
            print_header:  if print csv header, True or False
        Returns:
            pass
        Raises:
            some Exception
        """
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

    def invalidate_metadata(self, table_name):
        exec_string = 'invalidate metadata %s' % table_name
        try:
            self.cursor.execute(exec_string)
        except Exception as e:
            print exec_string, ' ERROR', e

    def refresh_table_nopart(self, table_name):
        exec_string = 'refresh %s' % table_name
        try:
            self.cursor.execute(exec_string)
        except Exception as e:
            print exec_string, ' ERROR', e

    def refresh_table_withpart(self, table_name, part_name):
        exec_string = """refresh %s partition(par_dt='%s')""" % table_name, part_name
        try:
            self.cursor.execute(exec_string)
        except Exception as e:
            print exec_string, ' ERROR', e


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
    #imp.export_csv(sql_string=sql_str, data_file=datafile, print_header=print_header)
    columns, sql_result = imp.execute_sql(sql_string=sql_str)
    print 'columns is:', columns
    print 'result is:', sql_result
    imp.refresh_table_nopart('bi_tmp.t_tmp_cai_20190906')
    imp._close()