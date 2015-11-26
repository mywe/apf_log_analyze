import sys, os, json, hashlib
from http.client import HTTPConnection

class TotalResult(object):
    def __init__(self, str_from, str_to):
        self.str_from = str_from
        self.str_to = str_to
        self.cnt_uv_d = 0
        self.cnt_uv_m = 0
        self.cnt_cusr_d = 0
        self.cnt_cusr_m = 0
        self.cnt_wusr_d = 0
        self.cnt_wusr_m = 0
        self.cnt_app_d = 0
        self.cnt_app_m = 0
        self.cnt_pv_d = 0
        self.cnt_pv_m = 0

    def getCnt(self, str_file):
        if os.path.exists(str_file) is False:
            return 0
        with open(str_file, "r") as ff:
            cnt = len(ff.readlines())
            if cnt > 1:
                cnt -= 1
            else:
                cnt = 0
            return cnt

    def getCntUv(self, str_file):
        return self.getCnt(str_file)

    def getCntClientUsr(self, str_file):
        if os.path.exists(str_file) is False:
            return 0
        with open(str_file, "r") as ff:
            arr_field = ff.readline().strip('\n').split('\t')
            idx_apf_addr = arr_field.index("apf_addr")
            cnt = 0
            for rr in ff.readlines():
                arr = rr.strip('\n').split('\t')
                if arr[idx_apf_addr].endswith('.C'):
                    cnt += 1

            return cnt

    def getCntWebUsr(self, str_file):
        if os.path.exists(str_file) is False:
            return 0
        with open(str_file, "r") as ff:
            arr_field = ff.readline().strip('\n').split('\t')
            idx_apf_addr = arr_field.index("apf_addr")
            cnt = 0
            for rr in ff.readlines():
                arr = rr.strip('\n').split('\t')
                if arr[idx_apf_addr].endswith('.W'):
                    cnt += 1

            return cnt

    def getCntApp(self, str_file):
        return self.getCnt(str_file)

    def getCntPv(self, str_file):
        if os.path.exists(str_file) is False:
            return 0
        with open(str_file, "r") as ff:
            arr_field = ff.readline().strip('\n').split('\t')
            idx_pv = arr_field.index('cnt_pv')
            cnt_pv = 0
            for rr in ff.readlines():
                arr = rr.strip('\n').split('\t')
                try:
                    cnt_pv += int(arr[idx_pv])
                except:
                    pass
            return cnt_pv

    def getResJson(self):
        res = dict()
        res["table_id"] = "565675087be48270cb759ae9"
        data = dict()
        ymd = self.str_to.split('-')
        data["f101"] = {"Y":int(ymd[0]), "M":int(ymd[1]), "D":int(ymd[2])}
        data["f102"] = self.cnt_uv_d
        data["f103"] = self.cnt_uv_m
        data["f104"] = self.cnt_cusr_d
        data["f105"] = self.cnt_cusr_m
        data["f106"] = self.cnt_wusr_d
        data["f107"] = self.cnt_wusr_m
        data["f108"] = self.cnt_app_d
        data["f109"] = self.cnt_app_m
        data["f110"] = self.cnt_pv_d
        data["f111"] = self.cnt_pv_m
        dataList = list()
        dataList.append(data)
        res["data"] = dataList
        return json.dumps(res)

    def do_statistics(self):
        self.cnt_uv_d = self.getCntUv("res/up_%s.txt"%(self.str_to))
        self.cnt_uv_m = self.getCntUv("res/up_%s_%s.txt"%(self.str_from, self.str_to))
        self.cnt_cusr_d = self.getCntClientUsr("res/wusr_%s.txt"%(self.str_to))
        self.cnt_cusr_m = self.getCntClientUsr("res/wusr_%s_%s.txt"%(self.str_from, self.str_to))
        self.cnt_wusr_d = self.getCntWebUsr("res/wusr_%s.txt"%(self.str_to))
        self.cnt_wusr_m = self.getCntWebUsr("res/wusr_%s_%s.txt"%(self.str_from, self.str_to))
        self.cnt_app_d = self.getCntApp("res/app_%s.txt"%(self.str_to))
        self.cnt_app_m = self.getCntApp("res/app_%s_%s.txt"%(self.str_from, self.str_to))
        self.cnt_pv_d = self.getCntPv("res/app_%s.txt"%(self.str_to))
        self.cnt_pv_m = self.getCntPv("res/app_%s_%s.txt"%(self.str_from, self.str_to))

def upload_statistics(str_from, str_to):
    res = TotalResult(str_from, str_to)
    res.do_statistics()
    params = res.getResJson()
    md5 = hashlib.md5()
    md5.update(params.encode('utf-8'))
    headers = {"Host":"apf.wps.cn","Content-type":"application/json",\
               "Content-Length":len(params),"X-Content-MD5":md5.hexdigest(),\
               "app_id":"ABVkEKbi4GiYScTPyH6m"}

    httpClient = HTTPConnection("apf.wps.cn", 80, timeout=30)
    httpClient.request("POST", "/table/push", params, headers)
    r1 = httpClient.getresponse()
    print(r1.status, r1.read())

#{"table_id":"565565457be48270c8759ad7","data":[{"f101":{"Y":1900,"M":1,"D":1,"h":0,"m":0,"ms":0},"f102":1,"f103":1,"f104":1,"f105":1,"f106":1,"f107":1,"f108":1,"f109":1,"f110":1,"f111":1}]}
