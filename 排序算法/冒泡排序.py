#!/usr/bin/python3
# -*- coding: utf-8 -*-
# @Time    : 2022/7/29 4:03 下午
# @Author  : dylantian
# @Software: PyCharm

# https://zhuanlan.zhihu.com/p/42586566
# 在相邻元素相等时，它们并不会交换位置，所以，冒泡排序是稳定排序。
# 在数据完全有序的时候展现出最优时间复杂度，为O(n)。其他情况下，几乎总是O(n^2)

# 算法描述
# 比较相邻的元素。如果第一个比第二个大，就交换它们两个；
# 对每一对相邻元素作同样的工作，从开始第一对到结尾的最后一对，这样在最后的元素应该会是最大的数；
# 针对所有的元素重复以上的步骤，除了最后一个；
# 重复步骤1~3，直到排序完成。

def bubbleSort(data):
    len_data = len(data)
    for i in range(len_data-1, -1, -1):
        for j in range(0, i):
            if data[j] > data[j+1]:
                tmp = data[j]
                data[j] = data[j+1]
                data[j+1] = tmp

data = [2, 4, 6, 8, 5, 3, 1]


bubbleSort(data)
print(data)