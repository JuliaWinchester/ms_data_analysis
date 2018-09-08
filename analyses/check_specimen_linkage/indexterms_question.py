import sys, os
sys.path.append(os.path.abspath(os.path.join('..', '..', 'lib')))

import db
import pandas
import requests

def get_json_field(json, field):
	if type(field) is list or type(field) is tuple:
		if field[0] in json:
			new_field = field[1:]
			if len(new_field) == 1:
				new_field = new_field[0]	
			return get_json_field(json[field[0]], new_field)
		else:
			return ''
	else:
		if field in json:
			return json[field]
		else:
			return ''

def get_db_field(row, field):
	if field in row:
		return str(row[field]).lower()
	else:
		return ''

conn = db.db_conn()
c = conn.cursor()
sql = """ 
		SELECT * FROM ms_specimens AS s
		JOIN ms_specimens_x_taxonomy AS sxt ON sxt.specimen_id = s.specimen_id
		JOIN ms_taxonomy_names AS t ON t.alt_id = sxt.alt_id
		WHERE s.uuid != ''
	 """

r = db.db_execute(c, sql)

for s in r:
	if s['uuid']:
		resp = requests.get('https://search.idigbio.org/v2/view/records/' + s['uuid'])
		json = resp.json()
		if get_db_field(s, 'genus') != get_json_field(json, ['data', 'dwc:genus']):
			print 'MS: ' + get_db_field(s, 'genus') + '     iDigBio: ' + get_json_field(json, ['data', 'dwc:genus'])

