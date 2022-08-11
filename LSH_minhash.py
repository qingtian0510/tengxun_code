#!/usr/bin/python3
# -*- coding: utf-8 -*-
# @Time    : 2022/7/14 5:26 下午
# @Author  : dylantian
# @Software: PyCharm

#!/usr/bin/env python
# coding=utf-8

from optparse import OptionParser
from pyspark import SparkContext,StorageLevel
import math
import json
import sys
import numpy as np

import time
import datetime
from datetime import date, timedelta
from datasketch import MinHashLSH, MinHash

def isValidUser(x):
    num = len(x)
    min_click_num = g_min_click_num.value
    max_click_num = g_max_click_num.value
    if num >= min_click_num and num <= max_click_num:
        return True
    return False

def getMinHash(vs):
    m = MinHash(num_perm=128)
    for v in vs:
        m.update(v.encode('utf-8'))
    return m

def getMinHashAndUpdateKey(k, vs):
    m = getMinHash(vs)
    k = k + "|" + ";".join(vs)
    return (k, m)

def splitHashBand(k, mhs):
    threshold = g_threshold.value
    mlsh = MinHashLSH(threshold=threshold, num_perm=128)
    res = mlsh.map2Value(k, mhs)
    return res

def getLocalSensitiveHash(xs):
    threshold = g_threshold.value
    mlsh = g_mlsh.value
    res = []
    for k, mhs in xs:
        res.extend(mlsh.map2Value(k, mhs))
    return res

def getUserPair(xs):
    l = len(xs)
    N = 100
    M = 10000
    candidates = xs
    if l > N:
        candidates = np.random.choice(xs[:2*N], N, False)
    if l > M:
        xs = np.random.choice(xs[:2*M], M, False)
    res = []
    for x in xs:
        for y in candidates:
            if x != y:
                res.append((x, y))
    return res

def getRelatedUserList(xs):
    l = len(xs)
    N = 50 # 100
    M = 10000
    candidates = xs
    if l == 1:
        return list()
    if l > N:
        candidates = np.random.choice(xs[:2*N], N, False)
    if l > M:
        xs = np.random.choice(xs[:2*M], M, False)
    res = []
    for x in xs:
        res.append((x, list(candidates)))
    return res

def getCosineDistance(x, y):
    s_x, s_y = set(x), set(y)
    l_x = len(s_x)
    l_y = len(s_y)
    dis = 0
    cover = s_x & s_y
    if l_x > 0 and l_y > 0:
        dis = len(cover) * 1.0 / math.sqrt(l_x * l_y)
    #    return round(dis, 1)
    return round(dis, 2)

def calCosineDistance(k, xs):
    ws = k.split("|")
    if len(ws) != 2:
        return None
    a_k, a_v = k.split("|")
    a_v = a_v.split(";")
    res = []
    for x in xs:
        ws = x.split("|")
        if len(ws) != 2:
            continue
        b_k, b_v = x.split("|")
        b_v = b_v.split(";")
        if a_k == b_k:
            continue
        dis = getCosineDistance(a_v, b_v)
        res.append(b_k + "|" + str(dis))
    return (a_k, res)

def sortByCosineDistance(xs):
    res_sorted = sorted(xs, key = lambda t: float(t.split("|")[1]), reverse = True)
    #topN = 100
    topN = 500
    res_sorted = res_sorted[:topN]
    return res_sorted

def filterWeakRelatedUsers(xs):
    threshold_related = 0.08
    res = [x for x in xs if float(x.split("|")[1]) >= threshold_related]
    return res

if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option("--i", "--user_click_input", dest="user_click_input", help="")
    parser.add_option("--o", "--output", dest="output", help="")
    parser.add_option("--tk", "--topk", dest="topk", help="output hdfs clk log file")
    parser.add_option("--maxc", "--max_click_num", dest="max_click_num", type="int", help="input qqnews_index file")
    parser.add_option("--minc", "--min_click_num", dest="min_click_num", type="int", help="input qqnews_index file")
    parser.add_option("--minu", "--min_user_num", dest="min_user_num", type="int", help="input qqnews_index file")
    parser.add_option("--maxu", "--max_user_num", dest="max_user_num", type="int", help="input qqnews_index file")
    parser.add_option("--mini", "--min_item_num", dest="min_item_num", type="int", help="min common item num")
    parser.add_option("--ts", "--threshold", dest="threshold", type="float", help="input qqnews_index file")
    parser.add_option("--pa", "--parallelism", dest="parallelism", type="int", help="parallelism number")

    (options, args) = parser.parse_args()

    allplay_input = options.user_click_input
    print("allplay_input is:" + allplay_input)
    print("user_click_outpath is:" + options.output)
    print("threshold is:" + str(options.threshold))

    sc = SparkContext()

    g_max_click_num = sc.broadcast(options.max_click_num)
    g_min_click_num = sc.broadcast(options.min_click_num)
    g_min_user_num = sc.broadcast(options.min_user_num)
    g_max_user_num = sc.broadcast(options.max_user_num)
    g_min_item_num = sc.broadcast(options.min_item_num)
    g_threshold = sc.broadcast(options.threshold)

    mlsh = MinHashLSH(threshold=options.threshold, num_perm=128)
    print ("band: ", mlsh.b, "r: ", mlsh.r)
    g_mlsh = sc.broadcast(mlsh)

    Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
    URI = sc._gateway.jvm.java.net.URI
    FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
    fs = FileSystem.get(URI("mdfs://cloudhdfs/newsclient"), sc._jsc.hadoopConfiguration())

    if fs.exists(Path(options.output)):
        fs.delete(Path(options.output))

    num_part = 512
    play_data = sc.textFile(allplay_input).repartition(num_part)

    user_item_list = play_data.map(lambda line: (line.split('\t')[0], line.split('\t')[1].split(','))).filter(lambda x: isValidUser(x[1]))
    print ("user_total count: ", user_item_list.count())
    for x in user_item_list.take(10):
        print (x)

    user_mhs = user_item_list.map(lambda x: getMinHashAndUpdateKey(x[0], x[1]))
    #    print "user_mhs count: ", user_mhs.count()
    #    for x in user_mhs.take(10):
    #        print x

    lsh_list = user_mhs.mapPartitions(lambda xs: getLocalSensitiveHash(xs)).groupByKey().mapValues(list).filter(lambda x: len(x[1]) > 1)
    print ("lsh_list count: ", lsh_list.count())
    for x in lsh_list.take(100):
        print (x)

    #    user_list = lsh_list.flatMap(lambda (k,xs): getUserPair(xs)).groupByKey().mapValues(list)
    user_list = lsh_list.flatMap(lambda x: getRelatedUserList(x[1])).map(lambda x: calCosineDistance(x[0],x[1])).filter(lambda x: x!=None).mapValues(lambda xs: filterWeakRelatedUsers(xs))
    print ("user_list count: ", user_list.count())
    for x in user_list.take(100):
        print (x)

    #    user_list = user_list.reduceByKey(lambda x,y: x + y).mapValues(lambda xs: np.unique(xs))
    #    print "user_list merge count: ", user_list.count()
    #    for x in user_list.take(100):
    #        print x

    user_list_sorted = user_list.reduceByKey(lambda x,y: x + y).mapValues(lambda xs: np.unique(xs)).mapValues(lambda xs: sortByCosineDistance(xs))
    print ("user_list_sorted count: ", user_list_sorted.count())
    for x in user_list_sorted.take(100):
        print (x)

    cnt = user_list_sorted.map(lambda x: x[0] + "\t" + ",".join(x[1]))
    cnt.saveAsTextFile(options.output, 'org.apache.hadoop.io.compress.GzipCodec')

    sc.stop()

