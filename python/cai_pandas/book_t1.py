#!/usr/bin/python
# -*- coding: UTF-8 -*-

import pandas as pd
import numpy as np

unames = ['user_id', 'gender', 'age', 'occupation', 'zip'] # 列名
users = pd.read_table('E://python_workspace/test_file/ml-1m/users.dat', sep='::', header=0, names=unames, engine='python')
rnames = ['user_id', 'movie_id', 'rating', 'timestamp']
ratings = pd.read_table('E:/python_workspace/test_file/ml-1m/ratings.dat', sep='::', header=0, names=rnames, engine='python')
mnames = ['movie_id', 'title', 'genres']
movies = pd.read_table('E:/python_workspace/test_file/ml-1m/movies.dat', sep='::', header=0, names=mnames, engine='python')

print users.head(5)
print ratings.info()
data = pd.merge(pd.merge(users, ratings), movies)
# pandas会自动判断连接的字段名，然后关联，所以关联字段必须名称一样
print data.info()
print data.head(5)
print data.ix[0]
# 按性别计算每部电影的平均得分
mean_rating =  data.pivot_table('rating', index='title', columns='gender', aggfunc='mean')
print mean_rating.head()
# title_size = data.groupby('title').size().reset_index(name='movie_rating_num')
title_size = data.groupby('title').size()   # <class 'pandas.core.series.Series'>
print title_size.head(5)
print type(title_size)  # <class 'pandas.core.frame.DataFrame'>
# 注意如果不用 reset_index(name=) 设置列名，则为 Series对象，否则是 dataframe 对象

active_title = title_size.index[title_size >= 250] # 筛选评分数大于250人的电影
print type(active_title)  # <class 'pandas.core.indexes.base.Index'>
print active_title

mean_rating = mean_rating.ix[active_title]
print mean_rating.head()

# 排序
top_female = mean_rating.sort_values(by='F', ascending=False) # 以F列排序，倒序
print top_female.head()

# 计算评分分歧
mean_rating['diff'] = mean_rating['M'] - mean_rating['F']
sorted_by_diff = mean_rating.sort_values(by='diff')
print sorted_by_diff.head(15)
print sorted_by_diff[::-1].head(10)   # 倒数10名

# 计算标准差
rating_std_by_title = data.groupby('title')['rating'].std()
# 过滤
rating_std_by_title = rating_std_by_title.ix[active_title]
# 降序排序
print rating_std_by_title.sort_values(ascending=False).head(10)