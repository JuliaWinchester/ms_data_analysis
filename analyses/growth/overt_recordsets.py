import sys, os
sys.path.append(os.path.abspath(os.path.join('..', '..', 'lib')))

import db
import ms_media_file
import requests
import cPickle as pickle
import csv

conn = db.db_conn()
c = conn.cursor()

sql = """ SELECT * FROM `ms_specimens`"""

r = db.db_query(c, sql)

recordsets = []

i = 0
for s in r:
	print i
	if s['uuid']:
		resp = requests.get('https://search.idigbio.org/v2/view/records/' + s['uuid'])
		json = resp.json()
		recordsets.append(json['indexTerms']['recordset'])
	i += 1
pickle.dump(recordsets, open('all_recordsets.p', 'wb'))

unique_recordsets = list(set(recordsets))
pickle.dump(unique_recordsets, open('unique_recordsets.p', 'wb'))

recordset_dict = {}
for r in unique_recordsets:
	resp = requests.get('https://search.idigbio.org/v2/view/recordsets/' + r)
	json = resp.json()
	recordset_dict[r] = json['data']['collection_name']
pickle.dump(recordset_dict, open('recordset_dict.p', 'wb'))

recordset_out = open('morphosource_recordsets.csv', 'w')
with recordset_out:
	writer = csv.writer(recordset_out)
	writer.writerow(['uuid','name'])
	for k,v in recordset_dict.iteritems():
		writer.writerow([k, v.encode('ascii', errors='ignore')])