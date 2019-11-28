# -*- coding: utf-8 -*-
"""
Created on Tue Sep 17 14:22:15 2019

@author: Administrator
"""

import pandas as pd
import os,sys
reload(sys)
sys.setdefaultencoding('utf-8')

newdir = 'C:\\Users\\Administrator\\Downloads\\dota2'
list = os.listdir(newdir)  # 列出文件夹下所有的目录与文件
print list

writer = pd.ExcelWriter('C:\\Users\\Administrator\\Downloads\\dota2_20191022.xlsx')

for i in range(0,len(list)):
    # writer = pd.ExcelWriter('C:\\Users\\Administrator\\Downloads\\duanyou.xlsx')
    csv_file = list[i].decode('gbk')
    print 'csv name:',csv_file
    data = pd.read_csv(newdir+'\\'+csv_file, encoding="utf-8",
                       index_col=0,engine='python',# delim_whitespace=True,
                       error_bad_lines=False,sep=',',dtype=str)
    data.to_excel(writer, sheet_name=csv_file,engine='xlsxwriter', encoding="gbk")
    # writer.save()

writer.save()