# -*- coding: UTF-8 -*-
import cx_Oracle as cx
import ConfigParser


class DBUtils(object):
    @staticmethod
    def getConnection(dbName='dt_sdb'):
        config = ConfigParser.RawConfigParser()
        confFile = 'D:\\datawarehouse\\cfg\\oracle_connect.config'
        config.read(confFile)
        ora_username = config.get(dbName, 'username')
        ora_password = config.get(dbName, 'password')
        ora_ip = config.get(dbName, 'ip')
        ora_host = config.get(dbName, 'host')
        ora_servname = config.get(dbName, 'service_name')
        cx_oar_link = '%s/%s@%s:%s/%s' % (ora_username, ora_password, ora_ip, ora_host, ora_servname)
        conn = cx.connect(cx_oar_link)
        return conn

    @staticmethod
    def selectSql(sql, conn=None):
        if not conn:
            conn = DBUtils.getConnection()
        cursor = conn.cursor()
        cursor.execute(sql)
        return cursor.fetchall()

    @staticmethod
    def insertList(sql, rowList, conn=None):
        if not conn:
            conn = DBUtils.getConnection()
        cursor = conn.cursor()
        cursor.executemany(sql, rowList)
        conn.commit()
        cursor.close()
        # conn.close()

    @staticmethod
    def insertSql(sql, conn=None):
        if not conn:
            conn = DBUtils.getConnection()
        cursor = conn.cursor()
        cursor.execute(sql)
        conn.commit()
        cursor.close()
        # conn.close()

    @staticmethod
    def deleteSql(sql, para_List=None, conn=None):
        if not conn:
            conn = DBUtils.getConnection()
        cursor = conn.cursor()
        if para_List:
            cursor.execute(sql, para_List)
        else:
            cursor.execute(sql)
        conn.commit()
        cursor.close()
        # conn.close()

    @staticmethod
    def updateSql(sql, rowlist, conn=None):
        if not conn:
            conn = DBUtils.getConnection()
        cursor = conn.cursor()
        cursor.execute(sql, rowlist)
        conn.commit()
        cursor.close()

    @staticmethod
    def closeconn(conn=None):
        if not conn:
            conn = DBUtils.getConnection()
        conn.close()


if __name__ == "__main__":
    v_sql = "select table_name from user_tables where rownum<10"
    ##result=DBUtils.selectSql(v_sql)
    ##print("result:%s"%result)
    ##sql="insert into quartz3.tmp_yrq_20190806(stuid,stuname) values(:1,:2)"
    rowlist = []
    rowlist.append((1, 'Tom'))
    rowlist.append((2, 'Mary'))
    rowlist.append((3, 'Tony'))
    ##DBUtils.insertList(sql,rowlist)
    ##DBUtils.insertSql("insert into quartz3.tmp_yrq_20190806(stuid,stuname) values(5,'Mally')")
    ##DBUtils.deleteSql("delete from quartz3.tmp_yrq_20190806 where stuid in(4,5)")