#!/usr/local/bin/python3.4
import sys, os, time, datetime
import subprocess, collections
from analyze_helper import *

accu_num = 1
accu_date = datetime.date.today() - datetime.timedelta(1)

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
    analyzeHelpers = AnalyzeHelper(date_from+datetime.timedelta(days=dd))
    analyzeHelpers.get_tal_coll(total_tal_coll)
    analyzeHelpers.getUpAndPeerColl(formerly_up_coll, formerly_peer_map)
    analyzeHelpers.getClientUserInfo(clientUserInfos)
    analyzeHelpers.getAppUserInfo(appUserInfos)

today_tal_coll = {}
analyzeHelpers = AnalyzeHelper(date_to)
analyzeHelpers.get_tal_coll(today_tal_coll)
analyzeHelpers.get_tal_coll(total_tal_coll)
analyzeHelpers.getUpAndPeerColl(today_up_coll, today_peer_map)
analyzeHelpers.getClientUserInfo(clientUserInfos)
analyzeHelpers.getAppUserInfo(appUserInfos)

todayClientUsrInfo = collections.defaultdict(lambda : ClientUserInfo)
todayAppUsrInfo = collections.defaultdict(lambda : AppUserInfo)
analyzeHelpers.getClientUserInfo(todayClientUsrInfo)
analyzeHelpers.getAppUserInfo(todayAppUsrInfo)

str_from = str(date_from)
str_to = str(date_to)
str_file = "table_%s_%s.txt"%(str_from, str_to)
if (accu_num == 1):
    str_file = "table_%s.txt"%(str_from)
outputTbAnalyzeRes(str_file, total_tal_coll)

formerly_usr_coll = collections.defaultdict(lambda:0)
getUsrColl(formerly_up_coll, formerly_peer_map, formerly_usr_coll)
today_usr_coll = collections.defaultdict(lambda:0)
getUsrColl(today_up_coll, today_peer_map, today_usr_coll)

total_usr_coll = formerly_usr_coll.copy()
newUserInfo = UserInfo()
todayUserInfo = UserInfo()
for uu in today_usr_coll:
    if uu in total_usr_coll:
        total_usr_coll[uu] = total_usr_coll[uu] + today_usr_coll[uu]
    else:
        total_usr_coll[uu] = today_usr_coll[uu]
        newUserInfo.add_user(uu)
    todayUserInfo.add_user(uu)

totalUserInfo = UserInfo()
for uu in total_usr_coll:
    totalUserInfo.add_user(uu)

str_file = "up_%s_%s.txt"%(str_from, str_to)
if (accu_num == 1):
    str_file = "up_%s.txt"%(str_from)
outputUpAnalyzeRes(str_file, total_usr_coll)

if len(clientUserInfos):
    outputClientUsrInfoRes("cusr_%s_%s.txt"%(str_from, str_to), clientUserInfos)
if len(todayClientUsrInfo):
    outputClientUsrInfoRes("cusr_%s.txt"%(str_to), todayClientUsrInfo)
if len(appUserInfos):
    outputAppUsrInfoRes("app_%s_%s.txt"%(str_from, str_to), appUserInfos)
if len(todayAppUsrInfo):
    outputAppUsrInfoRes("app_%s.txt"%(str_to), todayAppUsrInfo)

nNewTable = 0
nTotalVisit = 0
for (key,val) in today_tal_coll.items():
    if val.is_new:
        nNewTable += 1
    nTotalVisit += val.cnt_visit

nLargeThan20 = 0
for uu in today_usr_coll:
    if today_usr_coll[uu] > 20:
        nLargeThan20 += 1

nLargeThan50NewUsr = 0
for uu in newUserInfo.get_users():
    if today_usr_coll[uu] > 50:
        nLargeThan50NewUsr += 1

nLargeThan50In7Days = 0
for uu in today_usr_coll:
    if total_usr_coll[uu] > 50:
        nLargeThan50In7Days += 1

print("%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t" \
      %(todayUserInfo.get_usercnt(), newUserInfo.get_usercnt(), totalUserInfo.get_usercnt(),\
    todayUserInfo.get_clientusercnt(), newUserInfo.get_clientusercnt(), todayUserInfo.get_clientusercnt(),\
    nLargeThan20, nLargeThan50NewUsr, nLargeThan50In7Days, len(today_tal_coll), nNewTable, nTotalVisit))