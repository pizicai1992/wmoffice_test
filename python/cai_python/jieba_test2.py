#!/usr/bin/python
# -*- coding: UTF-8 -*-

from snownlp import SnowNLP as sn
import jieba
import cx_Oracle as cx
import os,re
from cai_python.getconn_util import get_conn
import sys
# sys.path.append('E:/cai_project/python/cai_python')
reload(sys)
sys.setdefaultencoding("utf-8")
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'


class GetOracleData():
    def __init__(self):
        self.ora_conn = get_conn('hive').cxoracle_link()
        self.db_conn = cx.connect(self.ora_conn, encoding="UTF-8", nencoding="UTF-8")
        self.cursor = self.db_conn.cursor()

    def get_standard_word(self, keycomment):
        __standard_word_sql_str = "select word_name_en,word_name_en_abbr,word_name_cn from t_dic_testcai_standardword " \
                                  "where word_name_cn= :key_comment" \
                    " or regexp_instr(word_name_cn_synonym, :key_comment)>0"
        self.cursor.prepare(__standard_word_sql_str)
        self.cursor.execute(None, {'key_comment': keycomment})
        keyword = self.cursor.fetchall()
        # print keyword
        cols = [d[0] for d in self.cursor.description]
        result = dict(zip(cols, keyword[0]))
        return result

    def get_root_word(self, keycomment):
        __root_word_sql_str = "select word_type,word_name_en,word_name_en_abbr,word_name_cn from t_dic_testcai_rootword " \
                                  "where word_name_cn= :key_comment" \
                    " or regexp_instr(word_name_cn_synonym, :key_comment)>0"
        self.cursor.prepare(__root_word_sql_str)
        self.cursor.execute(None, {'key_comment': keycomment})
        keyword = self.cursor.fetchall()
        cols = [d[0] for d in self.cursor.description]
        result = dict(zip(cols, keyword[0]))
        return result

    def conn_close(self):
        self.cursor.close()
        self.db_conn.close()


def key_name_split(input_key_name_cn):
    get_oradata = GetOracleData()
    jieba.del_word('总金额')
    jieba.load_userdict('E:/cai_project/python/test_file/keyword_dict.txt')
    seq_list = jieba.cut(input_key_name_cn.replace('的', ''))
    split_word_list = list(seq_list)
    standard_word_cn = []
    business_word_list = []
    kpi_word_list = []
    aggr_word_list = []
    cycle_word_list = []
    for key in range(len(split_word_list)):
        if re.match('\\d+', split_word_list[key]):
            split_word_list.append(split_word_list[key])
        else:
            root_result = get_oradata.get_root_word(split_word_list[key])
            standard_word_cn.append(root_result['WORD_NAME_CN'])
            if key > 0 and re.match('\\d+', split_word_list[key-1]) and root_result['WORD_TYPE'] == '周期修饰词':
                root_result['WORD_NAME_EN'] = split_word_list[key-1] + root_result['WORD_NAME_EN']
                root_result['WORD_NAME_CN'] = split_word_list[key - 1] + root_result['WORD_NAME_CN']
                root_result['WORD_NAME_EN_ABBR'] = split_word_list[key - 1] + root_result['WORD_NAME_EN_ABBR']
                cycle_word_list.append(root_result)
            elif root_result['WORD_TYPE'] == '周期修饰词':
                cycle_word_list.append(root_result)
            elif root_result["WORD_TYPE"] == '业务修饰词':
                business_word_list.append(root_result)
            elif root_result["WORD_TYPE"] == '指标修饰词':
                kpi_word_list.append(root_result)
            elif root_result["WORD_TYPE"] == '聚合修饰词':
                aggr_word_list.append(root_result)

    get_oradata.conn_close()
    standard_word_list = business_word_list + kpi_word_list + aggr_word_list + cycle_word_list
    standard_word_en = [x['WORD_NAME_EN'] for x in standard_word_list]
    standard_word_en_abbr = [x['WORD_NAME_EN_ABBR'] for x in standard_word_list]
    # print '_'.join(standard_word_en)
    # print '_'.join(standard_word_en_abbr)
    return '_'.join(standard_word_en), '_'.join(standard_word_en_abbr)


getora = GetOracleData()
input_key_cn = '异常的种类'
input_key = input_key_cn.replace('的', '')
res = getora.get_standard_word(input_key)

print '输入中文注释: %s' % input_key_cn
print '标准中文注释: %s' % res['WORD_NAME_CN']
print '标准英文单词: %s' % res['WORD_NAME_EN']
getora.conn_close()

print '*'*100
std_en, std_en_abbr = key_name_split('30日回流用户的充值总金额')
print '输入中文注释: 30日回流用户的充值总金额'
print '标准英文单词: %s' % std_en
print '标准英文缩写: %s' % std_en_abbr