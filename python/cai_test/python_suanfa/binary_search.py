# -*- coding: UTF-8 -*-

# 二分查找
# 给定一个递增的列表，和一个目标表，返回目标值在列表中最早出现的位置 

# def solution(list,index_list, target):
#     # index_list = range(len(list))
#     mid_index = len(index_list) // 2
#     mid_num = list[mid_index]

#     if len(index_list) == 1 and mid_num == target:
#         return mid_index
#     elif len(index_list) == 1 and mid_num != target:
#         return -1
#     elif len(index_list) > 1 and mid_num > target:
#         return solution(list, index_list[:mid_index], target)
#     elif len(index_list) > 1 and mid_num < target:
#         return solution(list, index_list[mid_index+1:], target)
#     # else:
#     #     return -1
    
#     # tmp_lens = len(list) // 2
#     # tmp = list[len(list)//2]
#     # if tmp == target:
#     #     return tmp_lens
#     # elif tmp > target:
#     #     return solution(list[:tmp_lens], target)
#     # else:
#     #     return solution(list[tmp_lens+1:], target)

def solution(list,head, tail, target):
    mid_index = (tail + head) // 2
    mid_num = list[mid_index]
    if head > tail:
        return -1
    
    if target == mid_num:
        return mid_index
    elif target < mid_num:
        return solution(list, head, mid_index-1, target)
    elif target > mid_num:
        return solution(list, mid_index+1, tail, target)

list_tmp = [1,2,3,5,5,6,7,9,12]
print solution(list_tmp,0, 8, 4)
