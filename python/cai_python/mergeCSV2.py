# -*- coding: utf-8 -*-
import pandas as pd
import os,sys
import pandas as pd
from openpyxl import load_workbook
import os
import xlsxwriter
reload(sys)
sys.setdefaultencoding('utf-8')

# os.chdir('E:/pycharm/Test/profile/')
input_dir = 'C:\\Users\\Administrator\\Downloads\\dota2'
output_dir = 'C:\\Users\\Administrator\\Downloads\\'

def _excelAddSheet(csv_name, excelWriter, sheet_name):
    dataframe = pd.read_csv(csv_name,encoding="utf-8",engine='python',delim_whitespace=True)
    # book = load_workbook(excelWriter.path)
    # excelWriter.book = book
    dataframe.to_excel(excel_writer=excelWriter, sheet_name=sheet_name,
                       index=None,engine='xlsxwriter', encoding="gbk")
    excelWriter.close()


excelWriter = pd.ExcelWriter(os.path.join(output_dir, 'dota2_20191022.xlsx'))
pd.DataFrame().to_excel(os.path.join(output_dir, 'dota2_20191022.xlsx'))
# excel必需已经存在，因此先建立一个空的sheet
list = os.listdir(input_dir)
for i in list:
    print 'CSV name:',i.decode('gbk')
    csvfile = os.path.join(input_dir, i.decode('gbk'))
    print csvfile
    sheet_name = i.decode('gbk')[:-4]
    print sheet_name
    _excelAddSheet(csvfile, excelWriter, sheet_name)

