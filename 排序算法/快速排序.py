#!/usr/bin/python3
# -*- coding: utf-8 -*-
# @Time    : 2022/8/1 11:25 上午
# @Author  : dylantian
# @Software: PyCharm

# 算法描述
# 从数列中挑出一个元素，称为"基准"（pivot），
# 重新排序数列，所有比基准值小的元素摆放在基准前面，所有比基准值大的元素摆在基准后面（相同的数可以到任何一边）。在这个分区结束之后，该基准就处于数列的中间位置。这个称为分区（partition）操作。
# 递归地（recursively）把小于基准值元素的子数列和大于基准值元素的子数列排序。

def quickSort(data, left, right):
    if left >= right:
        return
    base = data[left]
    low = left
    high = right
    while low < high:
        while low < high and data[high] >= base:
            high -= 1
        data[low] = data[high]   #从右边开始找比base小的值填充到左边

        while low < high and data[low] <= base:
            low += 1
        data[high] = data[low] #从左边开始找比base大的值填充到右边

    data[low] = base
    quickSort(data, left, low-1)
    quickSort(data, low+1, right)

data = [2, 4, 6, 8, 5, 3, 1]

quickSort(data, 0, len(data)-1)
print(data)