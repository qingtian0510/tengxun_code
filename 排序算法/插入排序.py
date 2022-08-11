#!/usr/bin/python3
# -*- coding: utf-8 -*-
# @Time    : 2022/7/29 4:44 下午
# @Author  : dylantian
# @Software: PyCharm

# 算法描述
# 把待排序的数组分成已排序和未排序两部分，初始的时候把第一个元素认为是已排好序的。
# 从第二个元素开始，在已排好序的子数组中寻找到该元素合适的位置并插入该位置。
# 重复上述过程直到最后一个元素被插入有序子数组中。

# 稳定排序，复杂度O(n^2)

def insertSort(data):
    len_data = len(data)
    for i in range(0, len_data-1):
        for j in range(i+1, 0, -1):
            if data[j] < data[j-1]:
                tmp = data[j]
                data[j] = data[j-1]
                data[j-1] = tmp
            else:
                break

data = [2, 4, 6, 8, 5, 3, 1]

insertSort(data)
print(data)
