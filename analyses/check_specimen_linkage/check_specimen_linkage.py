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


def check():
	conn = db.db_conn()
	c = conn.cursor()
	sql = """ 
			SELECT * FROM `ms_specimens` AS s
			LEFT JOIN ms_specimens_x_taxonomy AS x ON x.specimen_id = s.specimen_id
			LEFT JOIN ms_taxonomy_names AS n ON n.alt_id = x.alt_id
		 """

	r = db.db_execute(c, sql)

	mislinked = pandas.DataFrame(columns=
    ['specimen_id',
    'MS_institution_code',
    'iDB_institution_code',
    'MS_collection_code',
    'iDB_collection_code',
    'MS_catalog_number',
    'iDB_catalog_number',
    'MS_genus',
    'iDB_genus',
    'MS_species',
    'iDB_species'
    ])

 	# mislinked = pandas.read_csv('mislinked_specimens.csv')

	for s in r:
		if s['uuid']:
			if int(s['specimen_id']) in list(mislinked['specimen_id']):
				continue
			print(s['uuid'])
			resp = requests.get('https://search.idigbio.org/v2/view/records/' + s['uuid'])
			json = resp.json()

			# check institution code, collection code, specimen number, genus, and species
			if (get_db_field(s, 'institution_code') != get_json_field(json, ['indexTerms', 'institutioncode']).lower() or
				get_db_field(s, 'collection_code') != get_json_field(json, ['indexTerms', 'collectioncode']).lower() or
				get_db_field(s, 'catalog_number') != get_json_field(json, ['indexTerms', 'catalognumber']).lower() or
				get_db_field(s, 'genus') != get_json_field(json, ['indexTerms', 'genus']).lower() or
				get_db_field(s, 'species') != get_json_field(json, ['indexTerms', 'specificepithet']).lower()):
				print ('Found mislinked specimen')
				row = {
					'specimen_id': s['specimen_id'],
    				'MS_institution_code': s['institution_code'],
    				'MS_collection_code': s['collection_code'],
    				'MS_catalog_number': s['catalog_number'],
    				'MS_genus': s['genus'],
    				'MS_species': s['species'],
    				'iDB_institution_code': get_json_field(json, ['indexTerms', 'institutioncode']),
    				'iDB_collection_code': get_json_field(json, ['indexTerms', 'collectioncode']),
    				'iDB_catalog_number': get_json_field(json, ['indexTerms', 'catalognumber']),
    				'iDB_genus': get_json_field(json, ['indexTerms', 'genus']),
    				'iDB_species': get_json_field(json, ['indexTerms', 'specificepithet'])
					}
				mislinked = mislinked.append(row, ignore_index=True)
				mislinked.to_csv('mislinked_specimens_it.csv', index=False, index_label=False)
	

if __name__ == '__main__':
	check()
	
	

