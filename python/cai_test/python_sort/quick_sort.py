# -*- coding: UTF-8 -*-
# 快速排序
# 对于一串序列，首先从中选取一个数，凡是小于这个数的值就被放在左边一摞，
# 凡是大于这个数的值就被放在右边一摞。然后，继续对左右两摞进行快速排序。
# 直到进行快速排序的序列长度小于 2 （即序列中只有一个值或者空值）

def quick_sort(list_seq):
    if len(list_seq) < 2:
        return list_seq
    else:
        baseNum = list_seq[0]
        left_seq = [num for num in list_seq[1:] if num < baseNum]
        right_sqp = [num for num in list_seq[1:] if num >= baseNum]
        return quick_sort(left_seq) + [baseNum] + quick_sort(right_sqp)

seq = [6,5,4,8,9,15,15,42,31,22]
print quick_sort(seq)