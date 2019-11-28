#!/usr/bin/python
# -*- coding: UTF-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

import openpyxl
from oracle_util import DBUtils as oracle

wb = openpyxl.load_workbook('C:/Users/Administrator/Desktop/Info.xlsx')
sheets = wb.sheetnames
print(sheets, type(sheets))
for i in sheets:
    print 'sheet name is:',str(i)

sheet = wb.get_sheet_by_name('端游hive开发活动')
print sheet.title
result_list = []

for row in sheet.rows:
    cell_List = []
    for cell in row:
        cell_List.append(str(cell.value).decode('utf-8'))
        print cell.value,'\t',
    print
    result_list.append(cell_List)
###
print '********************************************'
# col_range = sheet['D:H']
# col_list = list(col_range)
# del col_list[2]
# for col in col_list:
#     print
#     for cell in col[1:]:
#         print cell.value
print result_list
del result_list[0]
for i in result_list:
    for j in i:
        print 'cell is:',j

cx_ora_link = 'mysql/mysql@10.14.250.51:1521/dfdb'
insert_sql="insert into T_ODI_HIVESYN_CONFIG " \
           "VALUES (:GAMEID,:GAMENAME,:MONTH,:EVENT_URL,to_date(:START_TIME,'YYYY-MM-DD HH24:MI:SS')," \
           "to_date(:END_TIME,'YYYY-MM-DD HH24:MI:SS')," \
           ":EVENT_TIME,:TARGET_TABLE,:SYN_NAME)"
conn = oracle.getConnection('mysql')
print conn
oracle.insertList(insert_sql,result_list,conn=conn)
print '*'*50