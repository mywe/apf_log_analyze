import os, datetime
from httplib import HTTPConnection

class TotalResult(object):
	"""docstring for TotalResult"""
	def __init__(self, date_from, date_to):
		self.date_from = date_from
		self.date_to = date_to
		self.cnt_uv_d = 0
		self.cnt_uv_m = 0
		self.cnt_used_d = 0
		self.cnt_used_m = 0
		self.cnt_cusr_d = 0
		self.cnt_cusr_m = 0
		self.cnt_wusr_d = 0
		self.cnt_wusr_m = 0
		self.cnt_app_d = 0
		self.cnt_app_m = 0
		self.cnt_pv_d = 0
		self.cnt_pv_m = 0

	def getResJson(self):
		pass

	def do_statistics():
		pass

def upload_statistics(date_from, date_to):
	res = TotalResult(date_from, date_to)
	params = urllib.urlencode({'name': 'tom', 'age': 22})
    headers = {"Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain"}
 
    httpClient = HTTPConnection("localhost", 80, timeout=30)
    httpClient.request("POST", "/test.php", params, headers)
