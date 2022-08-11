#!/usr/bin/python3
# -*- coding: utf-8 -*-
# @Time    : 2022/7/30 11:08 下午
# @Author  : dylantian
# @Software: PyCharm


# 算法描述
# 先将整个待排序的记录序列分割成为若干子序列分别进行直接插入排序，具体算法描述：
#
# 选择一个增量序列t1，t2，…，tk，其中ti>tj，tk=1；
# 按增量序列个数k，对序列进行 k 趟排序；
# 每趟排序，根据对应的增量ti，将待排序列分割成若干长度为m 的子序列，分别对各子表进行直接插入排序。仅增量因子为1 时，整个序列作为一个表来处理，表长度即为整个序列的长度。

#插入排序的升级版

#不稳定，复杂度O(nlogn) -> log -log2

def shellSort(data):
    n = len(data)
    gap = n//2

    while gap > 0:
        for i in range(n):
            j = i
            while j >= gap and data[j-gap] > data[j]:
                data[j], data[j-gap] = data[j-gap], data[j]
                j -= gap
        gap = gap//2


data = [2, 4, 6, 8, 5, 3, 1]
shellSort(data)
print(data)