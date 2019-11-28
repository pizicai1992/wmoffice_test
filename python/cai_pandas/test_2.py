#!/usr/bin/python
# -*- coding: UTF-8 -*-

import pandas as pd

movie_pd = pd.read_csv('E:/python_workspace/test_file/douban_movie.csv',header=0,sep='\t')
print type(movie_pd)
# 按列创建dataframe，用一个字典
temp_dic = {'score':[ 8.9, 8.2, 9.3],'category':['悬疑', '动作', '爱情']}
temp_pd = pd.DataFrame(temp_dic)
print temp_pd
# 按行创建 创建时需要传入一个列表进去
row1 = [8.9, '悬疑']
row2 = [8.2, '动作']
row3 = [9.3, '爱情']
temp_pd2 = pd.DataFrame([row1,row2,row3],columns=['score','category'])
print temp_pd2

print 'lengths is ',len(temp_pd)
print '索引是 :', temp_pd.index
# 改变索引
temp_pd.index = ['movie_1', 'movie_2', 'movie_3']
print '列名是: ', temp_pd.columns
# 改变列名
temp_pd.columns = ['movie_score', 'movie_category']
print temp_pd
print temp_pd.values

"""
    index 可以获取 DataFrame 的索引，更改之前是 0、1、2，之后变为了movie_1、movie_2 、movie_3 。
    columns 可以获取 DataFrame 的列名，更改之前是 score 、category，之后变为了movie_score、movie_category。
    values 则可以获取 DataFrame 的值，每一行各个列的值都是一个列表，所有的行整体又组成一个列表，有点类似于二维数组。
    同时也可以通过赋值的方式更改 DataFrame 的索引 index、列名 columns。
    DataFrame 类似于 Excel 中的表，有行和列
    index 索引、columns 列名 、values 值
    pd.DataFrame( ) 创建一个新的 DataFrame，可以传入字典或列表
"""