#!/usr/bin/python
# -*- coding: UTF-8 -*-

import pandas as pd

movie_pd = pd.read_csv('E:/python_workspace/test_file/douban_movie.csv',header=0,sep='\t')
# 按行筛选， loc( ) 函数，可以按照索引进行筛选
print movie_pd.loc[0]
print movie_pd.loc[range(10)]
print movie_pd.loc[[1,3,4]]
# 按列筛选
#print movie_pd['title']
#print movie_pd[['title', 'score']]
# 按一行一列或多行多列筛选，也是借助 loc( ) 函数，但这个时候需要通过传入两个参数
print movie_pd.loc[5,'actors']  # loc[5, 'actors'] 表示筛选出索引值是 5 的行，且列名是 actors 的列
print movie_pd.loc[[1,5,8],['title', 'actors']] # 表示筛选出索引值是 1、5、8 的行，且列名是 title 和 actors 的列
# DataFrame 类似于 Excel 中的表，那么 Series 就是 Excel 表中的某一行或某一列的数据类型，多个有相同索引的 Series 就可以组成一个 DataFrame
# 所以，movie_pd['title'] 返回的是 name 属性值为 title 的 Series，
# 和 DataFrame 没有任何联系了；而 movie_pd[['title']] 返回的是列名为 title 的 DataFrame

# 按条件筛选
# 筛选电影类型是剧情的 title 和 score 两列
print movie_pd[movie_pd['category'] == '剧情'][['title', 'score']]
# 筛选电影排名小于等于 5 且评分高于 9.0 的 title 一列
print movie_pd[(movie_pd['score'] >= 9) & (movie_pd['rank'] <= 5)][['title']]

# 其他条件筛选
"""
isnull ( ) 函数：筛选空值
notnull ( ) 函数：筛选非空值
isin( ) 函数：筛选某个字段的值在给定列表中
"""
print movie_pd[movie_pd['url'].isnull()]
print movie_pd[movie_pd['regions'].notnull()]
print movie_pd[movie_pd['score'].isin([3,5])]