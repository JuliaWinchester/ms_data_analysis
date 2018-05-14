import csv
import sys, os
sys.path.append(os.path.abspath(os.path.join('..', '..', 'lib')))
import warnings

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

def auto_relink_check():
	relink_success = pandas.DataFrame(columns=
	    ['specimen_id',
	    'MS_institution_code',
	    'MS_collection_code',
	    'MS_catalog_number',
	    'MS_genus',
	    'MS_species',
	    'Old_iDB_institution_code',
	    'Old_iDB_collection_code',
	    'Old_iDB_catalog_number',
	    'Old_iDB_genus',
	    'Old_iDB_species',
	    'New_iDB_institution_code',
	    'New_iDB_collection_code',
	    'New_iDB_catalog_number',
	    'New_iDB_genus',
	    'New_iDB_species',
	    'Old_iDB_uuid',
	    'New_iDB_uuid'
	    ])

	csv_df = pandas.read_csv('auto_candidates.csv')



	for specimen_id in csv_df['auto']:
		conn = db.db_conn()
		c = conn.cursor()
		sql = """ 
			SELECT * FROM `ms_specimens` AS s
			LEFT JOIN ms_specimens_x_taxonomy AS x ON x.specimen_id = s.specimen_id
			LEFT JOIN ms_taxonomy_names AS n ON n.alt_id = x.alt_id
			WHERE s.specimen_id = %s
		 	"""
		r = db.db_execute(c, sql, specimen_id)

		new_row = {}
		for s in r:
			# MS Values
			new_row['specimen_id'] = get_db_field(s, 'specimen_id')
			new_row['MS_institution_code'] = get_db_field(s, 'institution_code')
			new_row['MS_collection_code'] = get_db_field(s, 'collection_code')
			new_row['MS_catalog_number'] = get_db_field(s, 'catalog_number')
			new_row['MS_genus'] = get_db_field(s, 'genus')
			new_row['MS_species'] = get_db_field(s, 'species')

			# iDB Values
			if get_db_field(s, 'uuid'):
				print specimen_id
				old_idb = requests.get(
					'https://search.idigbio.org/v2/view/records/' +
					get_db_field(s, 'uuid')).json()
				new_row['Old_iDB_institution_code'] = get_json_field(old_idb, ['indexTerms', 'institutioncode'])
				new_row['Old_iDB_collection_code'] = get_json_field(old_idb, ['indexTerms', 'collectioncode'])
				new_row['Old_iDB_catalog_number'] = get_json_field(old_idb, ['indexTerms', 'catalognumber'])
				new_row['Old_iDB_genus'] = get_json_field(old_idb, ['indexTerms', 'genus'])
				new_row['Old_iDB_species'] = get_json_field(old_idb, ['indexTerms', 'specificepithet'])
				new_row['Old_iDB_uuid'] = get_json_field(old_idb, ['indexTerms', 'uuid'])
			if new_row['MS_catalog_number'] and new_row['MS_genus']:
				new_idb = requests.get(
					'https://search.idigbio.org/v2/search/records/?rq={"catalognumber": "' + 
					get_db_field(s, 'catalog_number') + '", "genus": "' + 
					get_db_field(s, 'genus') + '"}'
					).json()
				if int(new_idb['itemCount']) == 1:
					item = new_idb['items'][0]
					new_row['New_iDB_institution_code'] = get_json_field(item, ['indexTerms', 'institutioncode'])
					new_row['New_iDB_collection_code'] = get_json_field(item, ['indexTerms', 'collectioncode'])
					new_row['New_iDB_catalog_number'] = get_json_field(item, ['indexTerms', 'catalognumber'])
					new_row['New_iDB_genus'] = get_json_field(item, ['indexTerms', 'genus'])
					new_row['New_iDB_species'] = get_json_field(item, ['indexTerms', 'specificepithet'])
					new_row['New_iDB_uuid'] = get_json_field(item, ['indexTerms', 'uuid'])
				else:
					warnings.warn('WARNING: ' + str(new_idb['itemCount']) + ' items found for specimen ' + str(specimen_id))
			else:
				raise ValueError('No catalog number and genus for specimen ' + str(specimen_id))

			relink_success = relink_success.append(new_row, ignore_index=True)
			relink_success.to_csv('relink_success.csv', index=False, index_label=False)

	

if __name__ == '__main__':
	auto_relink_check()