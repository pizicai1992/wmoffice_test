#!/usr/bin/python
# -*- coding: UTF-8 -*-

import pandas as pd
import numpy as np
from datetime import datetime
movie_pd = pd.read_csv('E:/python_workspace/test_file/douban_movie.csv',header=0,sep='\t')

# groupby 分组求和
print movie_pd.groupby('category').size().head()
# 其类型为 Series，如果想要转化为 DataFrame 格式，同时给电影个数那一列添加列名 num，可以使用 reset_index( ) 函数，写法如下
print movie_pd.groupby('category').size().reset_index(name = 'num').tail()
# 有时候，SQL 中还会涉及到 count ( distinct movie_id ) 的去重计数操作，这个时候把 size( ) 函数替换为 nunique( ) 函数即可
print movie_pd.groupby('category')['id'].nunique().reset_index(name = 'num').tail()

# agg：辅助分组的函数
# 有时候按照某个字段分组以后，需要计算多个字段的值，这个时候就可以借助 agg 函数来实现。
# select id,
# max(score) as max_score,
# min(score) as min_score,
# avg(vote_count) as avg_count
# from movie
# group by id
agg_pd = movie_pd.groupby('id').agg({'score':[np.max,np.min], 'vote_count':np.mean}).reset_index()
print agg_pd.head()
print agg_pd.columns
# 上述结果是一个复合索引，单独来看的话，每个列名都是一个元组，接下来对列名重命名，元组的元素之间用下划线连接
# agg_pd.columns = agg_pd.columns.map(lambda x:'_'.join(x))
agg_pd.columns = agg_pd.columns.map(lambda x: '_'.join(x) if x[1] else x[0])
print agg_pd.head()

# 多字段分组聚合
# select category, id,
# avg(score) as avg_score,
# max(vote_count) as max_count
# from movie
# group by category, id

print movie_pd.groupby(['category', 'id']).agg({'score':np.mean,'vote_count':np.max}).reset_index().head()
# 排序使用 sort_values( ) 函数
print movie_pd.groupby(['category', 'id']).\
    agg({'score':np.mean,'vote_count':np.max}).\
    reset_index().sort_values('score',ascending = False).head()
"""
groupby( ) 分组运算
reset_index( ) 重置索引并可以更改列名
agg( ) 辅助多字段分组
sort_values( ) 按照字段排序
"""