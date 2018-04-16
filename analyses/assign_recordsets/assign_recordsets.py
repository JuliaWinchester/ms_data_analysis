import sys, os
sys.path.append(os.path.abspath(os.path.join('..', '..', 'lib')))

import db
import requests

conn = db.db_conn_socket()
c = conn.cursor()

sql = """ SELECT * FROM `ms_specimens` """

r = db.db_execute(c, sql)

for s in r:
	recordset = None
	if s['uuid']:
		resp = requests.get('https://search.idigbio.org/v2/view/records/' + s['uuid'])
		json = resp.json()
		recordset = json['indexTerms']['recordset']
		print(recordset)
		sql = 'UPDATE `ms_specimens` SET `recordset` = "' + recordset + '" WHERE `specimen_id` = ' + str(s['specimen_id']) + ''
		print(sql)
		db.db_execute(c, sql)
	
	

