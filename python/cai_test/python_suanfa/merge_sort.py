# -*- coding: UTF-8 -*-

# 归并排序
# 将两个有序列表合并为一个新的有序列表 

def mergeSort(l1, l2):
    merge_list = []
    i = 0
    j = 0
    while i < len(l1) or j < len(l2):
        if l1[i] < l2[j]:
            merge_list.append(l1[i])
            i += 1
        else:
            merge_list.append(l2[j])
            j += 1
        
    return merge_list

la = [1,2,4,6,9]
lb = [2,4,6,7,8,11,34,35,56]
print mergeSort(la, lb)