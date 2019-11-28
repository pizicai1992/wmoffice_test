#!/usr/bin/python
# -*- coding: UTF-8 -*-

import pandas as pd
from datetime import datetime
movie_pd = pd.read_csv('E:/python_workspace/test_file/douban_movie.csv',header=0,sep='\t')
# 如果逻辑关系比较简单，可以直接对 Pandas 的两列进行操作得到新的一列
movie_pd['total_score'] = movie_pd['score'] * movie_pd['vote_count']
print movie_pd.head()

# 使用for循环对一列或多列进行处理得到新的一列
# 比如需要根据电影的评分增加电影评分等级 movie_level 新列 ，评分小于 7.5 分的等级是 B，7.5 到 9.0 之间的是 A，9.0 以上的是 S
level_list = list()
for i in movie_pd.index:
    score = movie_pd.loc[i, 'score']
    if score < 7.5:
        mlevel = 'B'
    elif 7.5 <= score <= 9.0:
        mlevel = 'A'
    else:
        mlevel = 'S'
    level_list.append(mlevel)
movie_pd['movie_level'] = pd.Series(level_list)
print movie_pd[['score', 'movie_level']].head()
# 建议先新建一个列表 movie_level_list，在 for 循环中依次处理完后添加到列表中，然后使用 pd.Series 的方式添加新列即可

# 使用map函数map( ) 函数：参数中可以传入字典，也可以使用 lambda 表达式
# 比如 is_playable 字段在 Pandas 中的值是 True/False
# 增加一列中文的新列，True 对应的值为 可以播放，False 对应的值为 不能播放
# 直接传入字典 {True: '可以播放', False: '不能播放'} 进去即可
movie_pd['playable_cn'] = movie_pd['is_playable'].map({True:'可以播放', False:'无法播放'})
print movie_pd[['is_playable', 'playable_cn']].head()
# 使用 lambda 表达式，其中的 x 就相当于 for 循环时每次的 score 值
movie_pd['want_watch'] = movie_pd['score'].map(lambda x: 1 if x > 9 else 0)
print movie_pd[['score', 'want_watch']].head()

# 根据电影的上映日期 release_date 和 评论人数 vote_count，计算每部电影每天的平均评价人数
movie_pd['release_date'] = pd.to_datetime(movie_pd['release_date'])
movie_pd['total_days'] = movie_pd['release_date'].map(lambda x: (datetime.now() - x).total_seconds()/(24*3600))
movie_pd['daily_vote'] = movie_pd['vote_count'] / movie_pd['total_days']
print movie_pd[['release_date', 'total_days', 'vote_count', 'daily_vote']].head()
# 首先，使用 to_datetime( ) 函数将 字符串类型 转化为日期；
# 然后使用 map( ) 函数计算电影上映日期距离现在的时间差，并转化为天数；
# 最后，vote_count 和 total_day 两列直接相除得到 每部电影每天的平均评价人数

# cut( ) 函数：完美解决根据变量值划分区间的问题
movie_pd['new_level'] = pd.cut(movie_pd['score'],bins=[0,7.5,9,float('Inf')], labels=['BB', 'AA', 'SS'], right=False)
print movie_pd[['score', 'new_level']].head()
# bins 参数为一个列表，表示划分区间的临界值，
# labels 为不同区间对应的值，right = False 表示前必后开，
# 默认为 前开后必，所以最终的区间为：[0, 7.5) 对应值为 B，[7.5,9.0) 对应值为 A，
# 9.0 及以上对应值为 S，float('Inf') 表示正无穷大

"""
map( ) : 参数可以传入字典 或 使用 lambda 表达式
to_datetime( )：将 字符串类型 转化为 日期类型
cut( ) : 对数值型变量划分区间
"""