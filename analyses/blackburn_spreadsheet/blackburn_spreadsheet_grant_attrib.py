import sys, os
sys.path.append(os.path.abspath(os.path.join('..', '..', 'lib')))

import db
import pandas

csv = pandas.read_csv('MorphoSource_oVert_update_3_15_18.csv')

for index, row in csv.iterrows():
	ms_code = row['media'][1:]
	ms_code = ms_code.split('-', 2)[0]
	grant_attrib = row['grant support']
	if type(ms_code) is str and ms_code and type(grant_attrib) is str and grant_attrib:
		print(ms_code)
		print(grant_attrib)
		conn = db.db_conn_socket()
		c = conn.cursor()
		sql = """ 
			UPDATE `ms_media`
			SET `grant_support` = %s
			WHERE `media_id` = %s
			"""
		db.db_execute(c, sql, [grant_attrib, ms_code])
