import sys, os, time, datetime
import collections

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

class ClientUserInfo(object):
    def __init__(self):
        self.cnt_used = 0
        self.tbs = set()
        self.apps = set()
    def getCntUsed(self):
        return self.cnt_used

    def getCntTb(self):
        return len(self.tbs)

    def getCntApp(self):
        return len(self.apps)

class AppUserInfo(object):
    def __init__(self):
        self.cnt_used = 0
        self.users = set()
        self.cnt_pv = 0

    def getCntUsed(self):
        return self.cnt_used

    def getCntUser(self):
        return len(self.users)

    def getCntPV(self):
        return self.cnt_pv

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


class AnalyzeHelper(object):
    arr_self_ip = ["113.106.106.3", "113.106.106.26", "113.106.106.29"]
    ignore_path = ['/logout', '/usr', '/oauth2']

    def __init__(self, date):
        self.idx_apf_addr = -1
        self.idx_table_id = -1
        self.idx_app_id = -1
        self.idx_path = -1
        self.idx_ip = -1
        self.idx_user = -1
        self.idx_usr_own = -1
        self.idx_version = -1

        self.str_file = str(date)
        try:
            with open(self.str_file, "rb") as ff:
                arr_field = ff.readline().decode('utf-8').split('\t')
                self.idx_apf_addr = arr_field.index("apf_addr")
                self.idx_table_id = arr_field.index("table_id")
                self.idx_app_id = arr_field.index("app_id")
                self.idx_path = arr_field.index("path")
                self.idx_ip = arr_field.index("ip")
                self.idx_user = arr_field.index("usr_id")
                self.idx_usr_own = arr_field.index("usr_own_table")
                self.idx_version = arr_field.index("apf_build_version")
        except:
            pass

    def getClientUserInfo(self, clientUInfos):
        if os.path.exists(self.str_file) is False:
            return
        if self.idx_version == -1:
            return
        with open(self.str_file, "rb") as ff:
            arr_field = ff.readline().decode('utf-8').split('\t')
            for rr in ff.readlines():
                arr = rr.decode('utf-8').split('\t')
                if arr[self.idx_apf_addr].endswith(('.C')) is False:
                    continue
                if arr[self.idx_ip].startswith(tuple(self.arr_self_ip)) or arr[self.idx_ip].endswith(
                        tuple(self.arr_self_ip)):
                    continue
                if len(arr[self.idx_version]) == 0:
                    continue
                cid = arr[self.idx_apf_addr]
                clientUInfo = None
                if cid in clientUInfos:
                    clientUInfo = clientUInfos[cid]
                else:
                    clientUInfo = ClientUserInfo()

                if len(arr[self.idx_table_id]):
                    clientUInfo.tbs.add(arr[self.idx_table_id])
                    clientUInfo.cnt_used += 1

                if len(arr[self.idx_app_id]):
                    clientUInfo.apps.add(arr[self.idx_app_id])
                elif arr[self.idx_path].startswith('/issue_doc/'):
                    clientUInfo.apps.add(arr[self.idx_path][len('/issue_doc/'):])

                clientUInfos[cid] = clientUInfo

    def getAppUserInfo(self, appUInfos):
        if os.path.exists(self.str_file) is False:
            return
        with open(self.str_file, "rb") as ff:
            arr_field = ff.readline().decode('utf-8').split('\t')
            for rr in ff.readlines():
                arr = rr.decode('utf-8').split('\t')
                if arr[self.idx_apf_addr].endswith(('.C')):
                    continue
                if arr[self.idx_apf_addr].endswith(('.W'))\
                        and (self.idx_version == -1 or len(arr[self.idx_version]) == 0):
                    continue
                if arr[self.idx_ip].startswith(tuple(self.arr_self_ip)) or arr[self.idx_ip].endswith(
                        tuple(self.arr_self_ip)):
                    continue
                app_id = arr[self.idx_app_id]
                if len(app_id) == 0:
                    if arr[self.idx_path].startswith('/form/'):
                        app_id = arr[self.idx_path].split('/')[2]
                    else:
                        continue
                appUInfo = None
                if app_id in appUInfos:
                    appUInfo = appUInfos[app_id]
                else:
                    appUInfo = AppUserInfo()

                if arr[self.idx_path].startswith('/form/'):
                    appUInfo.cnt_pv += 1
                appUInfo.cnt_used += 1

                if len(arr[self.idx_apf_addr]):
                    appUInfo.users.add(arr[self.idx_apf_addr])

                appUInfos[app_id] = appUInfo

    def get_tal_coll(self, tal_coll):
        if os.path.exists(self.str_file) is False:
            return
        with open(self.str_file, "rb") as ff:
            arr_field = ff.readline().decode('utf-8').split("\t")
            for rr in ff.readlines():
                arr = rr.decode('utf-8').split("\t")
                vv_table = arr[self.idx_table_id]
                vv_path = arr[self.idx_path]
                vv_usr = arr[self.idx_user]
                vv_peer = arr[self.idx_apf_addr]
                vv_usr_own = arr[self.idx_usr_own]
                vv_ip = arr[self.idx_ip]

                if arr[self.idx_apf_addr].endswith(('.C', '.W'))\
                        and (self.idx_version == -1 or len(arr[self.idx_version]) == 0):
                    continue
                if (len(vv_table) == 0 or vv_ip in self.arr_self_ip):
                    continue

                if (vv_table not in tal_coll):
                    tal_coll[vv_table] = TableRecord(vv_table)

                tal_coll[vv_table].add_record(vv_peer, vv_path, vv_usr, vv_usr_own, vv_ip)

    def getUpAndPeerColl(self, up_coll, peer_map):
        if os.path.exists(self.str_file) is False:
            return
        if self.idx_version == -1:
            return
        with open(self.str_file, "rb") as ff:
            arr_field = ff.readline().decode('utf-8').split("\t")
            for rr in ff.readlines():
                arr = rr.decode('utf-8').split("\t")
                if len(arr[self.idx_version]) == 0:
                    continue;
                up = (arr[self.idx_user], arr[self.idx_apf_addr])
                if (arr[self.idx_ip].startswith(tuple(self.arr_self_ip)) or arr[self.idx_ip].endswith(
                        tuple(self.arr_self_ip))):
                    continue
                if (len(up[1]) == 0):
                    continue
                up_coll[up] = up_coll[up] + 1
                if (len(up[0]) != 0):
                    peer_map[up[1]].add(up[0])

def dateFmt(arg):
    fmts = ['%Y/%m/%d', '%Y-%m-%d', '%Y%m%d']
    for fmt in fmts:
        try:
            datetime.datetime.strptime(arg, fmt)
            return fmt
        except:
            continue
    return ''

def getUsrColl(up_coll, peer_map, usr_coll):
    for uu in up_coll:
        if (len(uu[0]) != 0):
            usr_coll[uu[0]] += up_coll[uu]
        elif(len(peer_map[uu[1]]) > 0):
            usr_coll[list(peer_map[uu[1]])[0]] += up_coll[uu]
        else:
            usr_coll[uu[1]] += up_coll[uu]

def outputTbAnalyzeRes(fileName, tal_coll):
    with open(fileName, "w") as ff:
        ff.write("table_id\tvisit_count\tuser_count\tpeer_c_count\tpeer_w_count\tis_create\t")
        ff.write("ip\tusr_id\n")
        for tbl_item in tal_coll.values():
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

def outputUpAnalyzeRes(fileName, up_coll):
    with open(fileName, "w") as ff:
        ff.write("usr_id\tcount\n")
        for uu in up_coll:
            ff.write("%s\t%d\n"%(uu, up_coll[uu]))

def outputClientUsrInfoRes(fileName, clientUsrInfos):
    with open(fileName, "w") as ff:
        ff.write("apf_addr\tcnt_used\tcnt_tb\tcnt_app\n")
        for usr in clientUsrInfos:
            ff.write("%s\t%d\t%d\t%d\n"%(usr, clientUsrInfos[usr].getCntUsed(),\
                                         clientUsrInfos[usr].getCntTb(), clientUsrInfos[usr].getCntApp()))

def outputWebUsr(fileName, peer_map):
    with open(fileName, "w") as ff:
        ff.write("apf_addr\n")
        for wusr in peer_map:
            ff.write("%s\n"%wusr)

def outputAppUsrInfoRes(fileName, appUsrInfos):
    with open(fileName, "w") as ff:
        ff.write("app_id\tcnt_used\tcnt_usr\tcnt_pv\n")
        for app in appUsrInfos:
            ff.write("%s\t%d\t%d\t%d\n"%(app, appUsrInfos[app].getCntUsed(),\
                                         appUsrInfos[app].getCntUser(), appUsrInfos[app].getCntPV()))
