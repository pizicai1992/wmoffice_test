#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""一个操作hive的模块"""
from impala.dbapi import connect
import argparse


class HiveUtil():
    """Connect hive to execute SQL and get result.

    连接hive执行查询并得到结果

    Attributes:
        db_name: 连接hive时的database，默认是default
    """

    def __init__(self, db_name='default'):
        self.dbname = db_name
        self.conn = connect(host='192.168.10.10', port=10182, auth_mechanism='PLAIN',
                            user='testuser', password='testuser_+-', database=self.dbname)
        self.cursor = self.conn.cursor()

    def execute_sql(self, sql, ddl=False):
        """Eexcute hive SQL.

        执行hive的查询语句并获得查询结果

        Args:
            sql: 需要执行的sql语句.
            ddl: sql语句类型是否是DDL语言，默认是false.

        Returns:
            如果是常规查询，返回的是查询结果.
            如果是DDL操作，则返回的是默认的成功信息.

        Raises:
            抛出连接时的异常.
        """
        try:
            self.cursor.execute(sql)
        except Exception as exc:
            op = self.cursor._last_operation
            log = op.get_log()
            print '[%s] execute error: \n %s' % (sql, exc)
            print log
        else:
            if not ddl:
                column = []
                result = []
                if self.cursor.description:
                    column = [data[0] for data in self.cursor.description]
                result = self.cursor.fetchall()
                return column, result
            else:
                return '[%s] execute success' % sql
        finally:
            self.cursor.close()
            self.conn.close()


def main():
    parser = argparse.ArgumentParser(
        description="execute hiveSQL", conflict_handler='resolve')
    parser.add_argument('-b', '--dbname', default='default',
                        help='hive的database,默认是default, example=bi_tmp')
    parser.add_argument('-s', '--sql', required=True,
                        help='执行的sql字符串, example=show tables')
    parser.add_argument('-d', '--ddl', default=False,
                        help='是否是DDL语句,默认是否, example=True')
    args = parser.parse_args()
    db_name = args.dbname
    sql = args.sql
    ddl = args.ddl
    hive_util = HiveUtil(db_name)
    result = hive_util.execute_sql(sql, ddl)
    print result


if __name__ == '__main__':
    main()
