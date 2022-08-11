#!/usr/bin/python3
# -*- coding: utf-8 -*-
# @Time    : 2022/8/1 4:57 下午
# @Author  : dylantian
# @Software: PyCharm


# 归并排序在于把序列拆分(至每个序列只有一个元素)再合并起来，使用分治法来实现，这就意味这要构造递归算法

#稳定，复杂度O(nlogn)

def merge(s1, s2, data):
    i = j = 0
    n = len(data)
    while i+j < n:
        if j == len(s2) or (i < len(s1) and s1[i] < s2[j]):
            data[i+j] = s1[i]
            i += 1
        else:
            data[i+j] = s2[j]
            j += 1


def mergeSort(data):
    n = len(data)
    # 剩一个或没有直接返回，不用排序
    if n < 2:
        return

    mid = n//2
    s1 = data[0:mid]
    s2 = data[mid:n]

    mergeSort(s1)
    mergeSort(s2)
    merge(s1, s2, data)

data = [2, 4, 6, 8, 5, 3, 1]
mergeSort(data)
print(data)


