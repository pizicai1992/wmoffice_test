#!/usr/bin/python
# -*- coding: UTF-8 -*-

import cx_Oracle as cx

cx_ora_link = 'bitask/viewsonic2010@10.14.250.9:1521/wmdw'
ora_sql = """
select column_name || '=' || decode(data_type,
                                        'NUMBER',
                                        '''decimal(38%2C10)''',
                                        'FLOAT',
                                        '''decimal(38%2C10)''',
                                        'DOUBLE',
                                        '''decimal(38%2C10)''',
                                        'VARCHAR2',
                                        'String',
                                        'DATE',
                                        'String',
                                        'String')
      from user_tab_columns
     where lower(table_name) = 't_000073_log_rolelogin'
     order by column_id
"""
table_name = 't_000073_log_rolelogin'
view_name = 'v'+table_name[1:]
column = ()
correct_column = ()
ora_column_is_Correct = 1
try:
    conn1 = cx.connect(cx_ora_link)
    cursor1 = conn1.cursor()
    cursor1.execute(ora_sql)
    result1 = cursor1.fetchall()
    print result1
    for resu in result1:
        print 'column info:',resu[0].split('=')[0]
        if resu[0].split('=')[0].endswith('_'):
            ora_column_is_Correct = 0
            print 'correct column info:', resu[0].split('=')[0][:-1]
            correct_column = correct_column + (resu[0].split('=')[0][:-1],)
        else:
            correct_column = correct_column + (resu[0].split('=')[0],)

        column = column + resu
    hive_columns = ','.join(column)
    correct_columns = ','.join(correct_column)
    createview_sql = 'create or replace view '+view_name+' as select '+correct_columns+' from '+table_name
    print hive_columns
    print correct_columns
    print createview_sql
    print 'ora_column_is_Correct: %d' % ora_column_is_Correct
except Exception, e:
    print e
finally:
    cursor1.close()
    conn1.close()
