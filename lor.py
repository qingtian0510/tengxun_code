#!/usr/bin/python3
# -*- coding: utf-8 -*-
# @Time    : 2022/7/18 4:45 下午
# @Author  : dylantian
# @Software: PyCharm

#!/usr/bin/env python
# coding=utf-8

#########################################################################
#
# Copyright (c) 2017 Tencent Inc. All Rights Reserved
#
#########################################################################


"""
File: log_odds_radio_spark.py
Author: tobyzeng@tencent.com
Date: 2018/01/30 18:01:36
Brief:
"""

from optparse import OptionParser
from pyspark import SparkContext,StorageLevel
from pyspark.sql import SparkSession
import math
import json
import sys

import time
import datetime
from datetime import date, timedelta

import subprocess
import redis

def rdsHandler():
    REDIS_HOST = "all.news_logoddsratio_item2item.redis.com"
    hostinfo = subprocess.check_output(["zkname", REDIS_HOST]).decode()
    hostsegs = hostinfo.strip().split('\t')
    ip = hostsegs[0]
    port = int(hostsegs[1])
    pool = redis.ConnectionPool(host=ip, port=port, db=0, password="z?GgQy8n/:UY/;Up0")
    r = redis.StrictRedis(connection_pool=pool)
    return r

def storeToRedis(x):
    r = rdsHandler()
    if r is None:
        r = rdsHandler()
    _pipe = r.pipeline(transaction=False)
    expiretime = 3*86400 #3天#
    count = 0
    for i in x:
        cmsid = i.split('\t')[0].strip()
        rels = i.split('\t')[1].strip()

        redisKey = "oddv#" + cmsid
        _pipe.setex(redisKey,expiretime,rels)
        count += 1
        if count % 5 == 0:
            _pipe.execute()
    _pipe.execute()



def get_show_uid(app_type, mac_imsi, android_id, devid):
    if (app_type != "android"):
        return  devid
    else:
        try:
            android_id_imsi = android_id.split("_")[1].strip()
        except:
            android_id_imsi = ""

        try:
            mac_imsi = mac_imsi.split("_")[1].strip()
        except:
            mac_imsi = ""

        imsi = mac_imsi
        if not imsi:
            imsi = android_id_imsi
        return devid + "_" + imsi

def parse_show_log(line):
    segs = line.strip().split(',')
    if len(segs) < 37:
        return None
    op = segs[5].strip()
    orig_id = segs[34].strip()
    if orig_id != '' or op == 'getSimpleNews':
        return None
    app_type = segs[27].strip()
    mac_imsi = segs[24].strip()
    android_id = segs[23].strip()
    dev_id   = segs[10].strip()
    uid = get_show_uid(app_type, mac_imsi, android_id, dev_id)
    if not uid:
        None
    rean_list = segs[14].strip()
    rean_list_ = rean_list.split("|")
    show_video_num = 0
    show_list = []
    for rean in rean_list_:
        rean_ = rean.split("-")
        if(len(rean_) < 4): continue
        id = rean_[0].strip()
        if len(id) < 9 : continue
        if id[8] != 'V': continue
        show_list.append(id)
        show_video_num = show_video_num + 1
    if (show_video_num > 0):
        return (uid, ','.join(show_list))
    else:
        return None

def parse_clk_log(line):
    dev_id = line["dev_id"]
    news_id = line["news_id"]
    article_type = line["iarticle_type"]
    if article_type != "0" and article_type != "1" and article_type != "4":
        return None

    is_click = line["content_click"]
    if not is_click:
        return None

    if len(news_id) < 9:
        return None

    return (dev_id, news_id)

def item_duplicate(tup):
    uid = tup[0]
    show_list = tup[1].data
    id_map = {}
    for id in show_list:
        id_map[id] = 1
    cnt = len(id_map)
    if cnt > g_max_click_num.value or cnt < g_min_click_num.value:
        return None
    return (uid, ','.join(id_map.keys()))

def item_split(tup):
    uid, clk_item = tup
    clk_list = clk_item.split(',')
    result = []
    for item in clk_list:
        result.append((item, uid))
    return result

def cal_item_user_num(tup):
    result = []
    item = tup[0]
    users = tup[1].data
    cnt = len(users)
    if cnt < g_min_user_num.value:
        return result
    for user in users:
        result.append((user, item + "_" + str(cnt)))
    return result

def make_related_item(tup):
    uid = tup[0]
    action_items = tup[1].data
    result = []
    for i in range(len(action_items)):
        item_i = action_items[i]
        for j in range(i + 1, len(action_items)):
            item_j = action_items[j]
            if item_i > item_j:
                result.append((item_i + '|' + item_j, uid))
            else:
                result.append((item_j + '|' + item_i, uid))
    return result

def cal_logoddsradio_score(tup):
    result = []
    try:
        clk_n11 = len(tup[1].data)
        item_list = tup[0].split('|')
        temp_a = item_list[0].split('_')
        temp_b = item_list[1].split('_')
        item_a = temp_a[0]
        clk_n1 = int(temp_a[1])
        item_b = temp_b[0]
        clk_n2 = int(temp_b[1])
        clk_n12 = clk_n1 - clk_n11
        clk_n21 = clk_n2 - clk_n11
        clk_n22 = g_total_user_num.value - clk_n11 - clk_n12 - clk_n21
        score_a2b = 0

        if clk_n11 >= g_min_user_num.value and clk_n22 > 0 and clk_n12 > 0 and clk_n21 > 0:
            score_a2b = math.log(clk_n11) + math.log(clk_n22) - math.log(clk_n12) - math.log(clk_n21)

        if score_a2b > 0:
            result.append((item_a, (item_b, score_a2b)))
            result.append((item_b, (item_a, score_a2b)))
    except:return list()
    return result

def rank_topk(tup):
    sim_list = tup[1].data
    simlist_sort = sorted(sim_list, key = lambda t: t[1], reverse = True)
    k = min(int(options.topk), len(simlist_sort))
    result = []
    #max_score = simlist_sort[0][1]

    for i in range(k):
        cid = simlist_sort[i][0].strip()
        score = simlist_sort[i][1]
        if score <= options.threshold:
            break
        result.append(cid + ':' + str(round(score, 4)))

    if len(result)==0:
        return None
    value = ','.join(result)
    return tup[0] + '\t' + value

def gen_dates(bdate,days):
    day = timedelta(days=-1)
    for i in range(days):
        yield bdate + day * i


def genInputFiles(inDir,daysNum):
    nowTI = datetime.datetime.now()
    bdate = datetime.date(nowTI.year,nowTI.month,nowTI.day)
    outlist = []
    for d in gen_dates(bdate,daysNum):
        strdate = d.strftime('%Y%m%d')
        _inpath = inDir + "/ds=" + strdate + "*"
        outlist.append(_inpath)

    if nowTI.hour == 0:
        return ",".join(outlist[1:])
    else:
        return ",".join(outlist)

def is_content_click(row):
    if row["eid"] == "em_item_article":
        return 1
    return 0

def is_content_like(row):
    if (row["eid"] == "em_up" and str(row["is_up"]) == "1" and
            ((row["element_path"] and row["element_path"].find("em_item_comment") != -1) or
             (row["dt_pg_path"] and row["dt_pg_path"].find("pg_cmt_detail") != -1) or
             (row["dt_pg_path"] and row["dt_pg_path"].find("pg_cmt_panel") != -1) or
             (row["dt_refpg_path"] and row["dt_refpg_path"].find("pg_cmt_panel") != -1) or
             row["pg_subtab_id"] == "cmt_reply")):
        return 0
    if row["eid"] == "em_up" and str(row["is_up"]) == "1":
        return 1
    return 0

def is_comment_like(row):
    if (row["eid"] == "em_up" and str(row["is_up"]) == "1" and
            ((row["element_path"] and row["element_path"].find("em_item_comment") != -1) or
             (row["dt_pg_path"] and row["dt_pg_path"].find("pg_cmt_detail") != -1) or
             (row["dt_pg_path"] and row["dt_pg_path"].find("pg_cmt_panel") != -1) or
             (row["dt_refpg_path"] and row["dt_refpg_path"].find("pg_cmt_panel") != -1) or
             row["pg_subtab_id"] == "cmt_reply")):
        return 1
    return 0

def is_collect(row):
    if str(row["is_favor"]) == "1" and row["eid"] == "em_favor":
        return 1
    return 0

def is_comment(row):
    if row["eid"] == "em_cmt_pub_btn":
        return 1
    return 0

def is_share(row):
    if row["eid"] and row["eid"][:9] == "em_share_":
        return 1
    return 0

def is_content_share(row):
    if row["eid"] and row["eid"][:9] == "em_share_" and row["element_path"] and row["element_path"].find("em_item_comment") == -1:
        return 1
    return 0

def is_comment_share(row):
    if row["eid"] and row["eid"][:9] == "em_share_" and row["element_path"] and row["element_path"].find("em_item_comment") != -1:
        return 1
    return 0


def parse_click_hi(row):
    hit_data = {}
    alg_info = {}
    if row["alg_info"]:
        try:
            alg_info = json.loads(row["alg_info"])
        except:
            alg_info = {}

    if not isinstance(alg_info, dict):
        alg_info = {}

    hit_data["trace_id"] = alg_info.get("seq_no", "").split('$')[0]

    idfv = row["idfv"]
    if not idfv: idfv = ""

    imei = row["android_id"]
    if not imei: imei = ""

    imsi = row["imsi"]
    if not imsi: imsi = ""

    if idfv:
        hit_data["dev_id"] = idfv
    else:
        hit_data["dev_id"] = imei + "_" + imsi

    hit_data["wxplugin_openid"] = ""
    if row["start_pluginext"]:
        wxplugin_openid_idx = row["start_pluginext"].find("o04IBA")
        if wxplugin_openid_idx != -1:
            hit_data["wxplugin_openid"] = row["start_pluginext"][wxplugin_openid_idx:]

    hit_data["qimei36"] = row["qimei36"]
    hit_data["news_id"] = row["article_id"]
    hit_data["said"] = row["article_id"]
    hit_data["iarticle_pos"] = row["article_real_pos"]
    hit_data["iarticle_type"] = row["article_type"]
    hit_data["time"] = row["event_time"]
    hit_data["chlid"] = row["chlid"]
    hit_data["sapptype"] = row["app_type"]
    hit_data["sorigid"] = row["pg_article_id"]
    hit_data["bucketId"] = alg_info.get("alg_version", "")

    hit_data["content_click"] = is_content_click(row)
    hit_data["content_like"] = is_content_like(row)
    hit_data["comment_like"] = is_comment_like(row)
    hit_data["collect"] = is_collect(row)
    hit_data["comment"] = is_comment(row)
    hit_data["share"] = is_share(row)
    hit_data["content_share"] = is_content_share(row)
    hit_data["comment_share"] = is_comment_share(row)

    #if str(hit_data["bucketId"])[:2] != "70":
    #    return None

    return hit_data

def deletPath(sc, path):
    Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
    URI = sc._gateway.jvm.java.net.URI
    FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
    fs = FileSystem.get(URI("mdfs://cloudhdfs/newsclient"), sc._jsc.hadoopConfiguration())

    if fs.exists(Path(path)):
        fs.delete(Path(path), True)

def parse_to_yotta(line):
    segs = line.split("\t")
    cmsid = segs[0].strip()
    value = segs[1].strip()
    key = "oddv#" + cmsid
    ttl = 0
    return "%s|%s|%s" % (key, value, ttl)

if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option("--ci", "--clk_input_dir", dest="clk_input_dir",
                      help="input hdfs clk log directory")
    parser.add_option("--dn", "--days_num", dest="days_num", type="int",
                      help="input days num")
    parser.add_option("--o", "--output", dest="output",
                      help="input hdfs clk log file")
    parser.add_option("--tk", "--topk", dest="topk",
                      help="output hdfs clk log file")
    parser.add_option("--lc", "--max_click_num", dest="max_click_num", type="int",
                      help="input qqnews_index file")
    parser.add_option("--lv", "--min_click_num", dest="min_click_num", type="int",
                      help="input qqnews_index file")
    parser.add_option("--hv", "--min_user_num", dest="min_user_num", type="int",
                      help="input qqnews_index file")
    parser.add_option("--ts", "--threshold", dest="threshold", type="float",
                      help="input qqnews_index file")
    parser.add_option("--kp", "--redis_key_prefix", dest="redis_key_prefix",
                      help="redis key prefix")
    parser.add_option("--pa", "--parallelism", dest="parallelism", type="int",
                      help="parallelism number")
    parser.add_option("--iwr", "--if_write_redis", dest="if_write_redis", type="int",
                      help="if write redis")
    parser.add_option("--oy", "--yotta_output", dest="yotta_output",
                      help="yottadb file")

    (options, args) = parser.parse_args()

    allclick_input = genInputFiles(options.clk_input_dir,options.days_num)
    print("allclick_input is:" + allclick_input)

    ss = SparkSession \
        .builder \
        .appName("lor") \
        .getOrCreate()

    # sc = SparkContext()
    sc = ss.sparkContext

    deletPath(sc, options.output)
    deletPath(sc, options.yotta_output)

    g_max_click_num = sc.broadcast(options.max_click_num)
    g_min_click_num = sc.broadcast(options.min_click_num)
    g_min_user_num = sc.broadcast(options.min_user_num)
    g_threshold = sc.broadcast(options.threshold)
    g_redis_key_prefix = sc.broadcast(options.redis_key_prefix)

    #parse_clk_log output format : (userid, cmsid)
    #after groupByKey: (userid, [cmsid,cmsid,...])

    date_str = options.output.split("/")[-1]
    # sql_click_hi = "select * from pcg_news_ods_v2.ods_news_dt_clck_hi where tdbank_imp_date=" + date_str

    w_h = 48
    t_s = (datetime.datetime.strptime(date_str, "%Y%m%d%H") + timedelta(hours=-w_h)).strftime("%Y%m%d%H")
    # t_e = (datetime.datetime.strptime(date_str, "%Y%m%d%H") + timedelta(hours=w_h)).strftime("%Y%m%d%H")
    t_e = date_str

    sql_click_hi = "select * from pcg_news_ods_v2.ods_news_dt_clck_hi where tdbank_imp_date between %s and %s" % (t_s, t_e)
    print ("sql: ", sql_click_hi)

    rdd_click_hi = ss.sql(sql_click_hi).rdd.map(parse_click_hi).filter(lambda x: x is not None)
    for x in rdd_click_hi.take(10):
        print (x)

    user_item_rdd_0 = rdd_click_hi.map(parse_clk_log) \
        .filter(lambda x: x != None) \
        .groupByKey().persist(StorageLevel.MEMORY_AND_DISK)

    #output format : (userid, "cmsid,cmsid,cmsid,...")
    user_item_rdd = user_item_rdd_0.map(item_duplicate) \
        .filter(lambda x: x != None) \
        .persist(StorageLevel.MEMORY_AND_DISK)
    user_item_rdd_0.unpersist()

    #output user num
    total_user_num = user_item_rdd.count()
    print('user count: %d' % total_user_num)
    g_total_user_num = sc.broadcast(total_user_num)

    #output format : (cmsid,[userid,userid,userid,...])
    item2uidListRdd = user_item_rdd.flatMap(item_split).groupByKey().persist(StorageLevel.MEMORY_AND_DISK)
    user_item_rdd.unpersist()
    print("item2uidListRdd count:%d" % item2uidListRdd.count())

    #cal_item_user_num output format : (userid, cmsid_10)  10 represent click num
    #after groupByKey : (userid, [cmsid_2,cmsid_8, cmsid_10,...])
    user2itemListRdd = item2uidListRdd.flatMap(cal_item_user_num).filter(lambda x: x != None).groupByKey().persist(StorageLevel.MEMORY_AND_DISK)
    item2uidListRdd.unpersist()

    print("user2itemListRdd count:" + str(user2itemListRdd.count()))

    #make_related_item output format : (item_i + '|' + item_j, uid)
    #after groupByKey : (item_i + '|' + item_j, [uid,uid,uid,...])
    relatedItemRdd = user2itemListRdd.flatMap(make_related_item).groupByKey().persist(StorageLevel.MEMORY_AND_DISK)
    user2itemListRdd.unpersist()

    print("relatedItemRdd count:" + str(relatedItemRdd.count()))
    ratioScoreRdd = relatedItemRdd.flatMap(cal_logoddsradio_score).filter(lambda x: x != None).groupByKey().persist(StorageLevel.MEMORY_AND_DISK)
    relatedItemRdd.unpersist()

    print("ratioScoreRdd count:" + str(ratioScoreRdd.count()))
    cnt = ratioScoreRdd.map(rank_topk).filter(lambda line : line is not None).repartition(options.parallelism).persist(StorageLevel.MEMORY_AND_DISK)
    ratioScoreRdd.unpersist()

    print("cnt count:" + str(cnt.count()))
    cnt.saveAsTextFile(options.output, 'org.apache.hadoop.io.compress.GzipCodec')

    cnt.map(parse_to_yotta).saveAsTextFile(options.yotta_output)

    if options.if_write_redis:
        cnt.foreachPartition(storeToRedis)
    sc.stop()


'''
--clk_input_dir=hdfs://qy-pcg-4-v3/user/tdw/warehouse/sz1_newsclient_interface.db/t_sh_atta_v2_zc000003620 --output=mdfs://cloudhdfs/newsclient/data/billzfwang/videoBottom/log_odds_ratio/%YYYYMMDDHH% --days_num=2 --topk=200 --max_click_num=2000 --min_click_num=5 --min_user_num=5 --threshold=1 --parallelism=200 --if_write_redis=1

--clk_input_dir=hdfs://qy-pcg-4-v3/user/tdw/warehouse/sz1_newsclient_interface.db/t_sh_atta_v2_zc000003620 --output=mdfs://cloudhdfs/newsclient/data/billzfwang/videoBottom/log_odds_ratio_v1/%YYYYMMDDHH% --days_num=2 --topk=200 --max_click_num=2000 --min_click_num=5 --min_user_num=5 --threshold=1 --parallelism=200 --if_write_redis=1

'''

