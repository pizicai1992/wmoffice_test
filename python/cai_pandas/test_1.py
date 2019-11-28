#!/usr/bin/python
# -*- coding: UTF-8 -*-

import pandas as pd

movie_pd = pd.read_csv('E:/python_workspace/test_file/douban_movie.csv',header=0,sep='\t')

"""这个输出的信息量很大，有索引、列名、列的数据类型 ( int64、bool、float64、object )。
可以看出电影数据共有 16 列，列名分别是：actor_count (主演的人数)、actors (主演列表)、category (分类)、
cover_url (封面图片网址)、id (电影id)、is_playable (是否可以播放)、is_watched (是否可以观看)、
rank (排名)、rating (评分, 含星级)、regions (制片国家)、release_date (上映日期)、
score (评分)、title (电影标题)、types (类型, 多个)、url (电影详情页网址)、vote_count (评价的人数)。"""
print movie_pd.info()

"""可以看出，对于变量类型是 int64 和 float64 的数值型变量，列出了个数、均值、方差、最小值、最大值和四分位数。
比如这些电影平均 vote_count 是 71773 人，最多有 875424 个人对某个电影进行了评分，电影平均 score 高达 8.5 分等"""
print movie_pd.describe()

"""
head( ) 方法会默认显示出 movie_pd 的前 5 行数据
tail( ) 方法会默认显示 movie_pd 的后 5 行数据
"""
print movie_pd.head()
print movie_pd.tail()

"""
   info( ) 查看数据有哪些字段和字段对应的数据类型
   describe( ) 对数值型变量进行统计性描述
   head( n ) 显示数据前 n 行
   tail( n ) 显示数据后 n 行
   header 表示从 csv 文件中读入数据时，是否把第一行作为列标题，head=0 表示把第一行作为列标题，head=None 表示没有列标题
"""
