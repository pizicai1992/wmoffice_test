#!/usr/bin/python
# -*- coding: UTF-8 -*-

import pandas as pd

movie_pd = pd.read_csv('E:/python_workspace/test_file/douban_movie.csv',header=0,sep='\t')

print movie_pd.info()  # info输出字段，类型，是否有空值等信息
print movie_pd.describe()  # describe 输出数值类型的统计信息，分别是：计数、平均值、方差、最小值、分位数、最大值

print movie_pd[movie_pd['id'] == 10455077 ][['types', 'category', 'rank', 'title']]

# Pandas 中删除数据使用 drop( ) 函数
# 按列删除,需要设置 axis 值为 1
new_movie_pd = movie_pd.drop(['category', 'rank'], axis=1)
# 按行删除：一般先获取到需要删除数据的索引，然后根据索引删除
# 比如要删除 regions 为 ['意大利'] 的数据，先通过 index 找到索引，
# 然后再做删除的操作，axis ＝0 为默认值，表示按行删除，不需要赋值
drop_index = movie_pd[movie_pd['regions'] == '[意大利]'].index
new_movie_pd = movie_pd.drop(drop_index)

# Pandas 中去重使用 drop_duplicates( ) 函数，和删除数据结合起来
new_movie_pd = movie_pd.drop(['category', 'rank'], axis=1)
new_movie_pd = new_movie_pd.drop_duplicates()

# 验证数据是否去重 nunique( ) 函数直接返回 id 去重后的个数，
# 等价于 len( new_movie_pd['id'].unique( ) )，即先对id去重，然后求个数
print len(new_movie_pd)
print new_movie_pd['id'].nunique()

# 去重后的数据存在有些电影重复出现的情况，否则电影的行数和 id 去重后的个数是相等的。通过如下操作可以找出这些电影的 id
movie_count = new_movie_pd.groupby('id').size().reset_index(name='count')
print movie_count[movie_count['count'] > 1]

"""
    drop( ) 按照列名 或 索引 删除数据
    drop_duplicates( ) 删除重复数据
    nunique( ) 返回某个字段去重后的个数
    unique( ) 对某个字段去重
"""