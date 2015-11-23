#!/usr/local/bin/python3.4
import sys, os, time, datetime
import subprocess, collections

def dateFmt(arg):
    fmts = ['%Y/%m/%d', '%Y-%m-%d', '%Y%m%d']
    for fmt in fmts:
        try:
            datetime.datetime.strptime(arg, fmt)
            return fmt
        except:
            continue
    return ''

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

class TableRecord(object):
    def __init__(self, vv_id):
        self.cnt_visit = 0
        self.set_usr = set()
        self.set_peer_c = set()
        self.set_peer_w = set()
        self.is_new = False
        self.id = vv_id
        self.ip = set()
        self.usr_id = set()

    def add_record(self, vv_peer, vv_path, vv_usr, vv_usr_own, vv_ip):
        if (len(vv_usr) != 0):
            self.set_usr.add(vv_usr)
        if (vv_peer.lower().endswith(".c")):
            self.set_peer_c.add(vv_peer)
        if (vv_peer.lower().endswith(".w")):
            self.set_peer_w.add(vv_peer)
        if (vv_path.lower().startswith("/table/createtable")):
            self.is_new = True
        self.cnt_visit = self.cnt_visit + 1
        if (vv_usr_own == "yes"):
            if (len(vv_ip) > 0):
                self.ip.add(vv_ip)
            if (len(vv_usr) > 0):
                self.usr_id.add(vv_usr)

def get_tal_coll(date, arr_self_ip, tal_coll):
    str_file = str(date)
    with open(str_file, "rb") as ff:
        arr_field = ff.readline().decode('utf-8').split("\t")
        idx_table = arr_field.index("table_id")
        idx_peer = arr_field.index("apf_addr")
        idx_path = arr_field.index("path")
        idx_usr = arr_field.index("usr_id")
        idx_usr_own = arr_field.index("usr_own_table")
        idx_ip = arr_field.index("ip")
        for rr in ff.readlines():
            arr = rr.decode('utf-8').split("\t")
            vv_table = arr[idx_table]
            vv_path = arr[idx_path]
            vv_usr = arr[idx_usr]
            vv_peer = arr[idx_peer]
            vv_usr_own = arr[idx_usr_own]
            vv_ip = arr[idx_ip]

            if (len(vv_table) == 0 or vv_ip in arr_self_ip):
                continue

            if (vv_table not in tal_coll):
                tal_coll[vv_table] = TableRecord(vv_table)

            tal_coll[vv_table].add_record(vv_peer, vv_path, vv_usr, vv_usr_own, vv_ip)

total_tal_coll = {}
arr_self_ip = ["113.106.106.3","113.106.106.26","113.106.106.29"]
date_from = accu_date - datetime.timedelta(days=accu_num-1)
date_to = accu_date
for dd in range(accu_num-1):
    get_tal_coll(date_from+datetime.timedelta(days=dd), arr_self_ip, total_tal_coll)

today_tal_coll = {}
get_tal_coll(date_to, arr_self_ip, today_tal_coll)
get_tal_coll(date_to, arr_self_ip, total_tal_coll)
nNewTable = 0
nTotalVisit = 0
for (key,val) in today_tal_coll.items():
    if val.is_new:
        nNewTable += 1
    nTotalVisit += val.cnt_visit

str_from = str(date_from)
str_to = str(date_to)
str_file = "table_%s_%s.txt"%(str_from, str_to)
if (accu_num == 1):
    str_file = "table_%s.txt"%(str_from)

with open(str_file, "w") as ff:
    ff.write("table_id\tvisit_count\tuser_count\tpeer_c_count\tpeer_w_count\tis_create\t")
    ff.write("ip\tusr_id\n")
    for tbl_item in total_tal_coll.values():
        ff.write(
            "%s\t%d\t%d\t%d\t%d\t"%
            (tbl_item.id,
            tbl_item.cnt_visit,
            len(tbl_item.set_usr),
            len(tbl_item.set_peer_c),
            len(tbl_item.set_peer_w)))

        ff.write(str(tbl_item.is_new)+"\t")
        ff.write(";".join(tbl_item.ip)+"\t")
        ff.write(";".join(tbl_item.usr_id)+"\n")

def getUpAndPeerColl(date, up_coll, peer_map):
    str_file = str(date)
    with open(str_file, "rb") as ff:
        arr_field = ff.readline().decode('utf-8').split("\t")
        idx_peer = arr_field.index("apf_addr")
        idx_usr = arr_field.index("usr_id")
        idx_ip = arr_field.index("ip")
        for rr in ff.readlines():
            arr = rr.decode('utf-8').split("\t")
            up = (arr[idx_usr], arr[idx_peer])
            if (arr[idx_ip].startswith(tuple(arr_self_ip)) or arr[idx_ip].endswith(tuple(arr_self_ip))):
                continue
            if (len(up[1])==0):
                continue
            up_coll[up] = up_coll[up] + 1
            if (len(up[0]) != 0):
                peer_map[up[1]].add(up[0])

def getUsrColl(up_coll, peer_map, usr_coll):
    for uu in up_coll:
        if (len(uu[0]) != 0):
            usr_coll[uu[0]] += up_coll[uu]
        elif(len(peer_map[uu[1]]) > 0):
            usr_coll[list(peer_map[uu[1]])[0]] += up_coll[uu]
        else:
            usr_coll[uu[1]] += up_coll[uu]

formerly_up_coll=collections.defaultdict(lambda:0)
formerly_peer_map = collections.defaultdict(lambda:set())
for dd in range(accu_num-1):
    getUpAndPeerColl(date_from+datetime.timedelta(days=dd), formerly_up_coll, formerly_peer_map)

today_up_coll = collections.defaultdict(lambda : 0)
today_peer_map = collections.defaultdict(lambda : set())
getUpAndPeerColl(date_to, today_up_coll, today_peer_map)

formerly_usr_coll = collections.defaultdict(lambda:0)
getUsrColl(formerly_up_coll, formerly_peer_map, formerly_usr_coll)
today_usr_coll = collections.defaultdict(lambda:0)
getUsrColl(today_up_coll, today_peer_map, today_usr_coll)

class UserInfo(object):
    def __init__(self):
        self.cnt_client = 0
        self.cnt_web = 0
        self.users = set()
    def add_user(self, uId):
        self.users.add(uId)
        if uId.lower().endswith(".w"):
            self.cnt_web += 1
        else:
            self.cnt_client += 1
    def get_clientusercnt(self):
        return  self.cnt_client
    def get_webusercnt(self):
        return  self.cnt_web
    def get_usercnt(self):
        return self.cnt_web + self.cnt_client
    def get_users(self):
        return self.users

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

str_file = "up_%s_%s.txt"%(str_from, str_to)
if (accu_num == 1):
    str_file = "up_%s.txt"%(str_from)

with open(str_file, "w") as ff:
    ff.write("usr_id\tcount\n")
    for uu in total_usr_coll:
        ff.write("%s\t%d\n"%(uu, total_usr_coll[uu]))

print("%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t" \
      %(todayUserInfo.get_usercnt(), newUserInfo.get_usercnt(), totalUserInfo.get_usercnt(),\
    todayUserInfo.get_clientusercnt(), newUserInfo.get_clientusercnt(), todayUserInfo.get_clientusercnt(),\
    nLargeThan20, nLargeThan50NewUsr, nLargeThan50In7Days, len(today_tal_coll), nNewTable, nTotalVisit))