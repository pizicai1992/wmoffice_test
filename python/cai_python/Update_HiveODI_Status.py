#!/usr/bin/python
# -*- coding: UTF-8 -*-
import sys
import openpyxl
from oracle_util import DBUtils as oracle
from getconn_util import log_util as logutil
from optparse import OptionParser
reload(sys)
sys.setdefaultencoding('utf-8')

log = logutil('updte_hiveodi_status.log')
# 1. 全量更新MySQL库的ODI配置表 T_ODI_HIVESYN_CONFIG
log.log_info('----Read Excel----')
wb = openpyxl.load_workbook('C:/Users/Administrator/Desktop/Info.xlsx')
sheets = wb.sheetnames
sheet = wb.get_sheet_by_name('端游hive开发活动')
result_list = []

for row in sheet.rows:
    cell_List = []
    for cell in row:
        cell_List.append(str(cell.value).decode('utf-8'))
        # print cell.value,'\t',
    # print
    result_list.append(cell_List)

del result_list[0]
# print result_list
delete_sql = "truncate table T_ODI_HIVESYN_CONFIG"
insert_sql = "insert into T_ODI_HIVESYN_CONFIG " \
           "VALUES (:GAMEID,:GAMENAME,:MONTH,:EVENT_URL,to_date(:START_TIME,'YYYY-MM-DD HH24:MI:SS')," \
           "to_date(:END_TIME,'YYYY-MM-DD HH24:MI:SS')," \
           ":EVENT_TIME,:TARGET_TABLE,:SYN_NAME)"
conn = oracle.getConnection('mysql')
oracle.deleteSql(delete_sql, conn=conn)
oracle.insertList(insert_sql, result_list, conn=conn)
oracle.closeconn(conn=conn)
log.log_info('----Config Updated----')

# 2. 根据传入的gameID 和 活动月份查找目标表和 SYN名称、开始和结束时间
def parse_command():
    usage = """
    python Update_HiveODI_Status.py -g 3 -m 1909 """
    parser = OptionParser(usage)
    parser.add_option("-g", "--gameid", dest="gameid",
                      help="Input the GameID")
    parser.add_option("-m", "--month", dest="month",
                      help="Input the ODI_month,format'YYMM' ")
    # Parse the command params
    (opts, args) = parser.parse_args()
    return opts.gameid, opts.month


def get_odi_info():
    gameid, month = parse_command()
    log.log_info('----Get ODI_info----')
    log.log_info('event game: %d, event month: %d' % (int(gameid),int(month)) )
    conn = oracle.getConnection('mysql')
    select_sql = "select target_table,syn_name," \
                 "to_char(start_time,'YYYY-MM-DD') start_time ," \
                 "to_char(end_time,'YYYY-MM-DD') end_time" \
                 " from t_odi_hivesyn_config where gameid="+str(gameid)+ \
                 " and month="+str(month)
    # print select_sql
    info_result = oracle.selectSql(select_sql,conn)
    oracle.closeconn(conn)
    return info_result
    # conn_q3 = oracle.getConnection('dt_sdb')
    # update_sql = ""
    # for i in info_result:
    #     print i


info_res = get_odi_info()


# 3. 修改任务的开始与结束时间,测试表: t_test_cwj_20190912
def update_work_status():
    log.log_info('----Update Chronus_Work----')
    conn = oracle.getConnection('dt_sdb')
    update_sql = """
    update chronus_work_works
    set start_time     = to_date(:s_time,'YYYY-MM-DD') + 1,
        end_time       = to_date(:e_time,'YYYY-MM-DD') + 2,
        next_exec_time = to_date(:s_time,'YYYY-MM-DD') + 1 + 2 / 24
    where split_parameters(parameters,'-t')=:syn_name
    """
    for i in info_res:
        para_dict = {}
        para_dict["s_time"] = i[2]
        para_dict["e_time"] = i[3]
        para_dict["syn_name"] = i[1]
        log.log_info('chronus_work parameters:'+ str(para_dict))
        oracle.updateSql(update_sql, para_dict, conn=conn)
    oracle.closeconn(conn)
    log.log_info('----Chronus_Work Updated----')


update_work_status()


# 4. 删除历史数据,测试表 t_test_cwj_20190912
def delete_testdata():
    log.log_info('----Delete TestData----')
    conn = oracle.getConnection('mysql')
    update_sql = """
    update t_dw_dashboard_kpi_proc_run set ignore_type=0,flag=token,begin_time=sysdate,end_time=sysdate 
     where proc_time<to_date(:s_time,'YYYY-MM-DD') and proc_name=:table_name
    """
    for i in info_res:
        del_sql = "delete from %s where logtime<to_date('%s','YYYY-MM-DD')" % (i[0], i[2])
        # print del_sql
        para_dict = {}
        para_dict["table_name"] = i[0]
        para_dict["s_time"] = i[2]
        log.log_info('delete parameters'+ str(para_dict))
        oracle.deleteSql(del_sql, conn=conn)
        oracle.updateSql(update_sql, rowlist=para_dict, conn=conn)
    oracle.closeconn(conn)
    log.log_info('----TestData Deleted----')


delete_testdata()