import sys, os, time, datetime
import collections

class ClientUserInfo(object):
	def __init__(self):
		self.cnt_used = 0
		self.tbs = set()
		self.apps = set()

class AppUserInfo(object):
	def __init__(self):
		self.cnt_used = 0
		self.users = set()
		self.cnt_pv = 0

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
    arr_self_ip = ["113.106.106.3","113.106.106.26","113.106.106.29"]
    ignore_path = ['/logout', '/usr', '/oauth2']

    def __init__(self, date):
        self.str_file = str(date)
        with open(self.str_file, "rb") as ff:
            arr_field = ff.readline().decode('utf-8').split('\t')
            self.idx_apf_addr = arr_field.index("apf_addr")
            self.idx_table_id = arr_field.index("table_id")
            self.idx_app_id = arr_field.index("app_id")
            self.idx_path = arr_field.index("path")
            self.idx_ip = arr_field.index("ip")
            self.idx_user = arr_field.index("usr_id")
            self.idx_usr_own = arr_field.index("usr_own_table")

    def getClientUserInfo(self, clientUInfos):
        with open(self.str_file, "rb") as ff:
            arr_field = ff.readline().decode('utf-8').split('\t')
            self.idx_apf_addr = arr_field.index("apf_addr")
            self.idx_table_id = arr_field.index("table_id")
            self.idx_app_id = arr_field.index("app_id")
            self.idx_path = arr_field.index("path")
            self.idx_ip = arr_field.index("ip")
            for rr in ff.readlines():
                arr = rr.decode('utf-8').split('\t')
                if arr[self.idx_apf_addr].endswith(('.C')) is False:
                    continue
                if arr[self.idx_ip].startswith(tuple(self.arr_self_ip)) or arr[self.idx_ip].endswith(tuple(self.arr_self_ip)):
                    continue
                if arr[self.idx_path].startswith(tuple(self.ignore_path)):
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
        with open(self.str_file, "rb") as ff:
            arr_field = ff.readline().decode('utf-8').split('\t')
            self.idx_apf_addr = arr_field.index("apf_addr")
            self.idx_table_id = arr_field.index("table_id")
            self.idx_app_id = arr_field.index("app_id")
            self.idx_path = arr_field.index("path")
            self.idx_ip = arr_field.index("ip")
            for rr in ff.readlines():
                arr = rr.decode('utf-8').split('\t')
                if arr[self.idx_apf_addr].endswith(('.C')):
                    continue
                if arr[self.idx_ip].startswith(tuple(self.arr_self_ip)) or arr[self.idx_ip].endswith(tuple(self.arr_self_ip)):
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
        with open(self.str_file, "rb") as ff:
            arr_field = ff.readline().decode('utf-8').split("\t")
            for rr in ff.readlines():
                arr = rr.decode('utf-8').split("\t")
                vv_table = arr[self.idx_table_id]
                vv_path = arr[self.idx_path]
                vv_usr = arr[self.idx_usr]
                vv_peer = arr[self.idx_apf_addr]
                vv_usr_own = arr[self.idx_usr_own]
                vv_ip = arr[self.idx_ip]

                if (len(vv_table) == 0 or vv_ip in self.arr_self_ip):
                    continue

                if (vv_table not in tal_coll):
                    tal_coll[vv_table] = TableRecord(vv_table)

                tal_coll[vv_table].add_record(vv_peer, vv_path, vv_usr, vv_usr_own, vv_ip)

    def getUpAndPeerColl(self, up_coll, peer_map):
        with open(self.str_file, "rb") as ff:
            arr_field = ff.readline().decode('utf-8').split("\t")
            for rr in ff.readlines():
                arr = rr.decode('utf-8').split("\t")
                up = (arr[self.idx_user], arr[self.idx_apf_addr])
                if (arr[self.idx_ip].startswith(tuple(self.arr_self_ip)) or arr[self.idx_ip].endswith(tuple(self.arr_self_ip))):
                    continue
                if (len(up[1])==0):
                    continue
                up_coll[up] = up_coll[up] + 1
                if (len(up[0]) != 0):
                    peer_map[up[1]].add(up[0])