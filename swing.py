#!/usr/bin/python3
# -*- coding: utf-8 -*-
# @Time    : 2022/7/14 7:36 下午
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

def cross_user_pair(x):
    item, vs = x
    num = len(vs)
    res = []
    for i in range(num):
        userA = vs[i]
        for j in range(i+1, num):
            userB = vs[j]
            if userA < userB:
                key = userA + "|" + userB
            else:
                key = userB + "|" + userA
            res.append((key, item))
    return res

def transform_list_to_dict(vs):
    user_dict = {}
    for u, i in vs:
        user_dict[u] = i
    return user_dict

def isValidUser(x):
    num = len(x)
    min_click_num = g_min_click_num.value
    max_click_num = g_max_click_num.value
    if num >= min_click_num and num <= max_click_num:
        return True
    return False

def isValidItem(x):
    num = len(x)
    min_user_num = g_min_user_num.value
    max_user_num = g_max_user_num.value
    if num >= min_user_num and num <= max_user_num:
        return True
    return False

def randomChooseUsers(vs):
    num = len(vs)
    N = 1000
    if num <= N:
        return vs
    else:
        return np.random.choice(vs, N, False)

def extend_item_list(k, vs):
    res = [(v, k) for v in vs]
    return res

def concateFiles(path, n):
    Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
    URI = sc._gateway.jvm.java.net.URI
    FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
    fs = FileSystem.get(URI("mdfs://cloudhdfs/newsclient"), sc._jsc.hadoopConfiguration())

    files = fs.listStatus(Path(path))
    files = files[-n:]

    res = []
    for f in files:
        res.append(f.getPath().toString())
    return ",".join(res)

def getItemPair(vs):
    sz = len(vs)
    alpha = 0.0
    weight = 1.0 / (alpha + sz)
    res = []
    for i in vs:
        for j in vs:
            if i == j:
                continue
            res.append((i + "|" + j, weight))
            res.append((j + "|" + i, weight))
    return res

def sortList(vs):
    vs = list(vs)
    vs_sorted = sorted(vs, key = lambda t: float(t.split(":")[1]), reverse = True)
    vs_sorted = vs_sorted[:300]
    return vs_sorted


if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option("--i", "--play_input_dir", dest="play_input_dir",
                      help="input hdfs video play log directory")
    parser.add_option("--o", "--icf_swing_output", dest="icf_swing_output",
                      help="ucf res output")
    parser.add_option("--maxc", "--max_click_num", dest="max_click_num", type="int",
                      help="input qqnews_index file")
    parser.add_option("--minc", "--min_click_num", dest="min_click_num", type="int",
                      help="input qqnews_index file")
    parser.add_option("--minu", "--min_user_num", dest="min_user_num", type="int",
                      help="input qqnews_index file")
    parser.add_option("--maxu", "--max_user_num", dest="max_user_num", type="int",
                      help="input qqnews_index file")
    parser.add_option("--pa", "--parallelism", dest="parallelism", type="int",
                      help="parallelism number")

    (options, args) = parser.parse_args()

    allplay_input = options.play_input_dir
    print("allplay_input is:" + allplay_input)
    print("icf_swing is:" + options.icf_swing_output)
    sc = SparkContext()

    g_max_click_num = sc.broadcast(options.max_click_num)
    g_min_click_num = sc.broadcast(options.min_click_num)
    g_min_user_num = sc.broadcast(options.min_user_num)
    g_max_user_num = sc.broadcast(options.max_user_num)

    Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
    URI = sc._gateway.jvm.java.net.URI
    FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
    fs = FileSystem.get(URI("mdfs://cloudhdfs/newsclient"), sc._jsc.hadoopConfiguration())

    if fs.exists(Path(options.icf_swing_output)):
        fs.delete(Path(options.icf_swing_output))

    allplay_input = concateFiles(options.play_input_dir, 24)
    #allplay_input = "mdfs://cloudhdfs/newsclient/user/shuandaoli/bottom_video/log_odds_radio/user_item_play_devid/2020040816/part-00000.gz"
    print("allplay_input_concated is:" + allplay_input)

    num_part = 512
    # optimization: after create RDD by textFile, do repartition
    play_data = sc.textFile(allplay_input).coalesce(num_part)
    #    print('num_partitions: ' + str(play_data.getNumPartitions()))

    def parsePlay(line):
        try:
            ws = line.split("\t")
            return (ws[1], ws[0])
        except:
            return None

    # (item, user)
    item_user_pair = play_data.map(lambda x: parsePlay(x)).filter(lambda x: x!=None)

    # (user, item_list)
    #user_item_list = item_user_pair.map(lambda (k,x): (x,k)).groupByKey().mapValues(list)
    user_item_list = item_user_pair.map(lambda x: (x[1],x[0])).groupByKey().mapValues(list)
    print ("user_total count: ", user_item_list.count())

    #user_item_list = user_item_list.filter(lambda (k,vs): isValidUser(vs))
    user_item_list = user_item_list.filter(lambda x: isValidUser(x[1]))

    # (user, item_num)
    user_click_statics = user_item_list.mapValues(lambda x: len(x))
    print ("user_click_statics count: ", user_click_statics.count())
    for x in user_click_statics.take(10):
        print (x)

    user_click_collect = user_click_statics.collect()
    user_click_dict = transform_list_to_dict(user_click_collect)
    g_user_click_dict = sc.broadcast(user_click_dict)
    #    print user_click_dict
    #    user = user_click_dict.keys()
    #    print "user count: ", len(user)

    #item_user_pair = user_item_list.flatMap(lambda (k,vs): extend_item_list(k, vs))
    item_user_pair = user_item_list.flatMap(lambda x: extend_item_list(x[0], x[1]))
    #    print "item_user_pair count: ", item_user_pair.count()
    #    for x in item_user_pair.take(10):
    #        print x

    def mergeUsers(x, y):
        sx = set(x)
        sy = set(y)
        sc = list(sx.union(sy))
        if len(sc) >= 10010:
            sc = sc[:10002]
        return sc

    item_user_list = item_user_pair.mapValues(lambda x: [x]).reduceByKey(lambda x,y: mergeUsers(x,y)).filter(lambda x: isValidItem(x[1])).mapValues(lambda vs: randomChooseUsers(vs))
    #    print ("item_user_list count: ", item_user_list.count())
    for x in item_user_list.take(10):
        print (x)

    user_pair_item = item_user_list.flatMap(lambda x: cross_user_pair(x)).groupByKey().mapValues(list)
    #    print "user_pair_item count: ", user_pair_item.count()
    #    for x in user_pair_item.take(10):
    #        print x

    user_pair_double_swing = user_pair_item.filter(lambda x: len(x[1]) > 1)
    #    cnt = user_pair_double_swing.map(lambda (k,vs): k + "\t" + ",".join(vs)).coalesce(options.parallelism)
    #    cnt.saveAsTextFile(options.cross_user_output, 'org.apache.hadoop.io.compress.GzipCodec')

    item_pair = user_pair_double_swing.flatMap(lambda x: getItemPair(x[1]))
    #    print "item pair size: ", item_pair.count()
    #    for x in item_pair.take(10):
    #        print x

    i2i_score = item_pair.reduceByKey(lambda x,y: x + y)
    #    print "i2i score size ", i2i_score.count()
    #    for x in i2i_score.take(10):
    #        print x

    #item_related_list = i2i_score.map(lambda (k,v): (k.split("|")[0], k.split("|")[1] + ":" + str(v))).groupByKey().mapValues(lambda vs: sortList(vs))
    item_related_list = i2i_score.map(lambda x: (x[0].split("|")[0], x[0].split("|")[1] + ":" + str(x[1]))).groupByKey().mapValues(lambda vs: sortList(vs))
    print ("item_related_list size: ", item_related_list.count())
    for x in item_related_list.take(10):
        print (x)

    cnt = item_related_list.map(lambda x: x[0] + "\t" + ",".join(x[1])).coalesce(options.parallelism)
    cnt.saveAsTextFile(options.icf_swing_output, 'org.apache.hadoop.io.compress.GzipCodec')

    sc.stop()

