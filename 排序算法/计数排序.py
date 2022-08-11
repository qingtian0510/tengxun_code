#!/usr/bin/python3
# -*- coding: utf-8 -*-
# @Time    : 2022/8/1 7:43 下午
# @Author  : dylantian
# @Software: PyCharm

# 算法步骤
# 找到待排序列表中的最大值 k，开辟一个长度为 k+1 的计数列表，计数列表中的值都为 0。
# 遍历待排序列表，如果遍历到的元素值为 i，则计数列表中索引 i 的值加1。
# 遍历完整个待排序列表，计数列表中索引 i 的值 j 表示 i 的个数为 j，统计出待排序列表中每个值的数量。
# 创建一个新列表（也可以清空原列表，在原列表中添加），遍历计数列表，依次在新列表中添加 j 个 i，新列表就是排好序后的列表，整个过程没有比较待排序列表中的数据大小。
#
# 当输入的元素是n个0 到 k 之间的整数时，它的运行时间是O(n+k)。
#
# 计数排序不是比较排序，排序的速度快于任何比较排序算法。
#
# 核心思想：统计每个整数在序列中出现的次数，进而推导出每个整数在有序序列中的索引

#稳定
# 最好、最坏、平均时间复杂度：O(n+k)
# 空间复杂度：O(n+k)


def countSort(data):
    n = len(data)
    if n < 2:
        return data

    max_value = max(data)
    count = [0 for _ in range(max_value+1)]
    for value in data:
        count[value] += 1

    data.clear()

    for ind, value in enumerate(count):
        for i in range(value):
            data.append(ind)

    return data

data = [2, 4, 6, 8, 5, 3, 1]

countSort(data)
print(data)

