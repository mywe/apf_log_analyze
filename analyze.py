#!/usr/local/bin/python3.4
import sys, os, time, datetime
import subprocess, collections
from analyze_helper import *
from upload_Analyze import upload_statistics

accu_num = 1
accu_date = datetime.date.today() - datetime.timedelta(1)
up_date_from = datetime.date(2015, 11, 9)
pv_date_from = datetime.date(2015, 11, 12)

for argx in sys.argv:
    fmt = dateFmt(argx)
    if (argx.isdigit()):
        accu_num = int(argx)
    elif len(fmt):
        accu_date = datetime.datetime.strptime(argx, fmt).date()

if (accu_date >= datetime.date.today()):
    print("there is no log for %s" %(str(accu_date)))
    sys.exit()

if (accu_num > 30 or accu_num <= 0):
    print("%d is too big or too small"%(accu_num))
    sys.exit()

total_tal_coll = {}
date_from = accu_date - datetime.timedelta(days=accu_num-1)
date_to = accu_date
formerly_up_coll=collections.defaultdict(lambda:0)
formerly_peer_map = collections.defaultdict(lambda:set())

today_up_coll = collections.defaultdict(lambda : 0)
today_peer_map = collections.defaultdict(lambda : set())
clientUserInfos = collections.defaultdict(lambda : ClientUserInfo())
appUserInfos = collections.defaultdict(lambda : AppUserInfo())

for dd in range(accu_num-1):
    tmpDate = date_from+datetime.timedelta(days=dd)
    analyzeHelpers = AnalyzeHelper(tmpDate)
    if tmpDate >= up_date_from:
        analyzeHelpers.get_tal_coll(total_tal_coll)
        analyzeHelpers.getUpAndPeerColl(formerly_up_coll, formerly_peer_map)
    if tmpDate >= pv_date_from:
        analyzeHelpers.getClientUserInfo(clientUserInfos)
        analyzeHelpers.getAppUserInfo(appUserInfos)

today_tal_coll = {}
analyzeHelpers = AnalyzeHelper(date_to)
if date_to >= up_date_from:
    analyzeHelpers.get_tal_coll(today_tal_coll)
    analyzeHelpers.get_tal_coll(total_tal_coll)
    analyzeHelpers.getUpAndPeerColl(today_up_coll, today_peer_map)

todayClientUsrInfo = collections.defaultdict(lambda : ClientUserInfo)
todayAppUsrInfo = collections.defaultdict(lambda : AppUserInfo)
if date_to >= pv_date_from:
    analyzeHelpers.getClientUserInfo(clientUserInfos)
    analyzeHelpers.getAppUserInfo(appUserInfos)
    analyzeHelpers.getClientUserInfo(todayClientUsrInfo)
    analyzeHelpers.getAppUserInfo(todayAppUsrInfo)

if os.path.exists('res') is False:
    os.mkdir('res')

str_from = str(date_from)
str_to = str(date_to)
str_file = "res/table_%s_%s.txt"%(str_from, str_to)
if (accu_num == 1):
    str_file = "res/table_%s.txt"%(str_from)
outputTbAnalyzeRes(str_file, total_tal_coll)

formerly_usr_coll = collections.defaultdict(lambda:0)
getUsrColl(formerly_up_coll, formerly_peer_map, formerly_usr_coll)
today_usr_coll = collections.defaultdict(lambda:0)
getUsrColl(today_up_coll, today_peer_map, today_usr_coll)
total_peer_map = formerly_peer_map.copy()
total_peer_map.update(today_peer_map)

total_usr_coll = formerly_usr_coll.copy()
for uu in today_usr_coll:
    if uu in total_usr_coll:
        total_usr_coll[uu] = total_usr_coll[uu] + today_usr_coll[uu]
    else:
        total_usr_coll[uu] = today_usr_coll[uu]

str_file = "res/up_%s_%s.txt"%(str_from, str_to)
if (accu_num == 1):
    str_file = "res/up_%s.txt"%(str_from)
outputUpAnalyzeRes(str_file, total_usr_coll)
if accu_num != 1:
    outputUpAnalyzeRes("res/up_%s.txt"%(str_to), today_usr_coll)

if len(clientUserInfos):
    outputClientUsrInfoRes("res/cusr_%s_%s.txt"%(str_from, str_to), clientUserInfos)
if len(todayClientUsrInfo):
    outputClientUsrInfoRes("res/cusr_%s.txt"%(str_to), todayClientUsrInfo)
if len(appUserInfos):
    outputAppUsrInfoRes("res/app_%s_%s.txt"%(str_from, str_to), appUserInfos)
if len(todayAppUsrInfo):
    outputAppUsrInfoRes("res/app_%s.txt"%(str_to), todayAppUsrInfo)
if len(total_peer_map):
    outputWebUsr("res/wusr_%s_%s.txt"%(str_from, str_to), total_peer_map)
if len(today_peer_map):
    outputWebUsr("res/wusr_%s.txt"%(str_to), today_peer_map)

upload_statistics(str_from, str_to)