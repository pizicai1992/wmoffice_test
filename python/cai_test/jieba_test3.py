#!/usr/bin/python
# -*- coding: UTF-8 -*-
'''
功能:将中文的关键字分割并与数据库中已有的关键字列表进行匹配
参数:-k '中文关键字'
示例:python keyword_split.py -k '付费用户留存率'
'''
from optparse import OptionParser
from snownlp import SnowNLP as sn
import jieba
import cx_Oracle as cx
import os
import sys
from ..cai_python.getconn_util import get_conn
reload(sys)
sys.setdefaultencoding("utf-8")
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

class ora_Until():
    def __init__(self):

        self.ora_conn = get_conn('hive').cxoracle_link()
        self.db_conn = cx.connect(self.ora_conn)
        self.cursor = self.db_conn.cursor()
        self.sql = "select keyword from t_dic_hivedw_keyword where key_comment = :key_comment"
        self.sql2 = "select COLUMN_NAME,column_type from t_dic_hivedw_columnname where column_comment = :key_comment"

    def get_column(self, keycomment):
        self.cursor.prepare(self.sql2)
        self.cursor.execute(None, {'key_comment': keycomment})
        keyword = self.cursor.fetchall()
        #print keyword
        if len(keyword) == 0:
            return None
        else:
            return keyword[0]

    def get_data(self, keycomment):

        self.cursor.prepare(self.sql)
        self.cursor.execute(None, {'key_comment': str(keycomment)})
        keyword = self.cursor.fetchall()
        if len(keyword) == 0:
            return None
        else:
            return keyword[0][0]

    def oraconn_close(self):
        self.cursor.close()
        self.db_conn.close()

class keyname_split():
    def __init__(self, keyname):
        self.keyname = keyname
        self.oraconn = ora_Until()

    def get_keyword(self):
        columnname = self.oraconn.get_column(self.keyname)
        if columnname is not None:
            print ' '.join(columnname)
        else:
            jieba.load_userdict('D:/mydict.txt')
            seq_list = jieba.cut(self.keyname)
            keylist = []
            for i in seq_list:
                if i == '用户数':
                    word = 'usernum'
                elif i == '的':
                    continue
                else:
                    word = self.oraconn.get_data(i)
                    if word is not None:
                        word = word
                    else:
                        s_word = sn(unicode(i.encode("utf-8"), 'utf-8'))
                        word = ''.join(s_word.pinyin)
                keylist.append(word)
            self.oraconn.oraconn_close()
            print '_'.join(keylist)+' string'

parser = OptionParser()
parser.add_option("-k", "--keyname", dest="keyname", help="关键字")
(options, args) = parser.parse_args()
v_keyname = options.keyname

if __name__ == '__main__':
    print '中文关键字是 %s' % v_keyname
    keysplit = keyname_split(v_keyname)
    print '匹配的关键字是 ,'
    keysplit.get_keyword()