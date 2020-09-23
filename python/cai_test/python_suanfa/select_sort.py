# -*- coding: UTF-8 -*-

# 选择排序
# 思路：
# 跟冒泡排序类似，不过选择排序每次能选取剩余元素中最小的元素放入有序区 
# https://www.jianshu.com/p/089fdf98ab2a
# 首先选定索引=0的元素作为初始元素跟后面的元素比较，只要比初始元素小，就跟他交换值，然后继续比较
# 直到首轮结束，这样就得到了最小的元素，依次这样比较，每轮都能选出剩余元素中的最小元素放入
# 有序区，知道结束循环

def selectSort(list):
    min_pos = 0
    sort_list = []
    for i in range(len(list)-1):
        min_pos = i
        print list[i]
        for j in range(i+1, len(list)):
            if list[j] < list[min_pos]:
                # min_num = list[j]
                list[i],list[j] = list[j],list[i]
        print 'list is:', list
        sort_list.append(list[min_pos])
    return list

listA = [5,3,7,32,4,1,9]
print selectSort(listA)
