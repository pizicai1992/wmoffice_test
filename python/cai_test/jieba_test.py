#!/usr/bin/python
# -*- coding: UTF-8 -*-
from optparse import OptionParser
from snownlp import SnowNLP as sn
import jieba
import cx_Oracle as cx
import os
import sys
from ..cai_python.getconn_util import get_conn
reload(sys)
sys.setdefaultencoding( "utf-8" )
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
'''
使用jieba分词插件测试中文分词
'''
parser = OptionParser()
parser.add_option("-k", "--keyname", dest="keyname", help="关键字")
(options, args) = parser.parse_args()
v_keyname = options.keyname

seq_list=jieba.cut(v_keyname)
key_list=[]
for i in seq_list:
    print i
    key_list.append(i)

print key_list
for j in key_list:
    print j

print key_list[0]
ora_conn = get_conn('hive').cxoracle_link()
db_conn = cx.connect(ora_conn)
cursor = db_conn.cursor()
cursor.prepare("select keyword from t_dic_hivedw_keyword where key_comment like :key_comment")
cursor.execute(None, {'key_comment':'%'+'当天'+'%'})
result = cursor.fetchall()
print 'relust', result
print 'length is :', len(result)
if len(result) == 0:
    s_word= sn(unicode('当天', 'utf-8'))
    print ''.join(s_word.pinyin)
    print 'kongzhi'

else:
    if result[0][0] == 'no_keyword':
        s_word = sn(unicode('当天', 'utf-8'))
        print 'no_keyword:',''.join(s_word.pinyin)
    print 'haha' #result[0][0]

cursor.close()
db_conn.close()
