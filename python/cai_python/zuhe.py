# -*- coding: UTF-8 -*-


def zuhe(source_array, begin, number, dest_array):
    if number == 0 :
        print dest_array
    if begin == len(source_array):
        return

    dest_array.append(source_array[begin])
    zuhe(source_array, begin+1, number-1, dest_array)
    dest_array.remove(source_array[begin])
    zuhe(source_array, begin + 1, number, dest_array)
    # print dest_array


s_list = [1,2,3,4,5]
d_list = []
num = 1
while num <= len(s_list):
    zuhe(s_list, 0, num, d_list)
    print d_list
    num += 1
