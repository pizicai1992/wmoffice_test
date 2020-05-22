#!/usr/bin/python
# -*- coding: UTF-8 -*-
import base64
import os

PIC_PATH = 'D:/笔记文件/markdown_pic'
UPIC_PATH = unicode(PIC_PATH,'utf-8')
pic_name = 'mapreduce_shuffle.png'
file_path = os.path.join(UPIC_PATH, pic_name)
f=open(file_path,'rb') #二进制方式打开图文件
ls_f=base64.b64encode(f.read()) #读取文件内容，转换为base64编码
f.close()
print(ls_f)