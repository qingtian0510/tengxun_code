#!/usr/bin/python3
# -*- coding: utf-8 -*-
# @Time    : 2022/7/29 4:28 下午
# @Author  : dylantian
# @Software: PyCharm
#
# 算法描述
# 在未排序序列中找到最小（大）元素，存放到排序序列的起始位置
# 从剩余未排序元素中继续寻找最小（大）元素，然后放到已排序序列的末尾。
# 重复第二步，直到所有元素均排序完毕。

# 稳定性：不稳定，复杂度O(n^2)
def selectSort(data):
    len_data = len(data)
    for i in range(len_data):
        min = i
        for j in range(i+1, len_data):
            if data[j] < data[min]:
                min = j
        tmp = data[i]
        data[i] = data[min]
        data[min] = tmp



data = [2, 4, 6, 8, 5, 3, 1]
selectSort(data)

print(data)