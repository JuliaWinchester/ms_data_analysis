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

def fix_specimen_links():
	success_csv = pandas.DataFrame(columns=
		['specimen_id',
		'success',
		'new_taxon'		
		])
	#success_csv = pandas.read_csv('post_relink_success.csv')

	specimen_csv = csv_df = pandas.read_csv('relink_success_correct.csv')

	for index, row in specimen_csv.iterrows():
		success_csv = pandas.read_csv('post_relink_success.csv')
		new_row = {}
		new_taxon = 0
		if row['Correct'] == 1 and row['New_iDB_uuid']:
			if row['Old_iDB_uuid'] == row['New_iDB_uuid']:
				new_row['specimen_id'] = row['specimen_id']
				new_row['success'] = 2
				new_row['new_taxon'] = new_taxon
			else:
				# Get iDB specimen record for new UUID
				idb = requests.get(
					'https://search.idigbio.org/v2/view/records/' +
					row['New_iDB_uuid']).json()
				if int(idb['itemCount']) == 1:
					item = idb['items'][0]

					# Do taxa differ?
					if get_json_field(item, ['indexTerms', 'genus']).lower() != row['MS_genus'].lower():
						# Taxa differ. Is there a MS taxon for the 'new' taxon?
						conn = db.db_conn()
						c = conn.cursor()
						sql = """
							SELECT * FFOM `ms_taxonomy` AS t
							JOIN `ms_taxonomy_names` AS n ON n.taxon_id = t.taxon_id
							WHERE n.genus = %s AND n.species = %s
							"""
						r = db.db_execute(c, sql, [
							get_json_field(item, ['indexTerms', 'genus']),
							get_json_field(item, ['indexTerms', 'specificepithet'])
							])

						if len(r) == 0:
							# Need to generate an all new taxon
							'''
							Steps:
								1) Delete the sxt link to the current taxon
								2) Create a new ms_taxonomy record for this taxon
								3) Create a new ms_taxonomy_names record for this taxon, using iDB
								4) Create a new txn link between the two tables
								5) Create a new sxt link from specimen to new taxon
								6) Change taxon_id 
							'''
						elif len(r) == 1:
							# Need to associate this specimen with currently existing new taxon
							# Steps: Get old taxon id, Get new taxon id, make sure there is not already a spec_x_taxon link to new one, if not, delete existing link and make the new one
							# Get old taxon_id
							conn = db.db_conn()
							c = conn.cursor()
							sql = """
								SELECT * FROM `ms_specimens_x_taxonomy` 
								WHERE specimen_id = %
								"""
							old_sxt = db.db_execute(c, sql, [row['specimen_id']])

							if len(old_sxt) == 1:
								old_taxon_id = old_sxt['taxon_id']
								old_link_id = old_sxt['link_id']
							else:
								raise ValueError('More than one sxt for specimen_id ' + row['specimen_id'])

							# Get new taxon_id
							new_taxon_id = r[0]['taxon_id']
							new_alt_id = r[0]['alt_id']

							# Is there already an sxt for the new_taxon_id?
							conn = db.db_conn()
							c = conn.cursor()
							sql = """
								SELECT * FROM `ms_specimens_x_taxonomy`
								WHERE specimen_id = %s AND taxon_id = %s
								"""
							new_sxt = db.db_execute(c, sql, [row['specimen_id'], new_taxon_id])

							if len(new_sxt) > 0:
								raise ValueError('Already existing link between taxon id ' + new_taxon_id + ' and specimen_id ' + row['specimen_id'])
							else:
								#delete old sxt
								conn = db.db_conn()
								c = conn.cursor()
								sql = """
									DELETE FROM `ms_specimens_x_taxonomy`
									WHERE link_id = %
								"""
								del_res = db.db_execute(c, sql, old_link_id)

								#create new sxt
								conn = db.db_conn()
								c = conn.cursor()
								sql = """
									INSERT INTO `ms_specimens_x_taxonomy`
									(specimen_id, taxon_id, alt_id, user_id)
									VALUES
									(%s, %s, %s, %s)
								"""
								ins_res = db.db_execute(c, sql, [row['specimen_id'], new_taxon_id, new_alt_id, 37])

								new_taxon = 1
						else:
							raise ValueError(str(len(r)) ' MS taxon records for specimen uuid' + row['New_iDB_uuid'])

					# Copy over specimen record values from iDB
					new_inst_code = get_json_field(item, ['indexTerms', 'institutioncode'])
					new_coll_code = get_json_field(item, ['indexTerms', 'collectioncode'])
					new_catalog_num = get_json_field(item, ['indexTerms', 'catalognumber'])
					new_uuid = get_json_field(item, ['indexTerms', 'uuid'])
					new_recordset = get_json_field(item, ['indexTerms', 'recordset'])
					val_array = [new_inst_code, new_coll_code, new_catalog_num, new_uuid, new_recordset, row['specimen_id']]

					conn = db.db_conn()
					c = conn.cursor()
					sql = """
						UPDATE `ms_specimens`
						SET `institution_code` = %s, `collection_code` = %s, `catalog_number` = %s, `uuid` = %s, `recordset` = %s
						WHERE specimen_id = %s
						"""
					r = db.db_execute(c, sql, val_array)

					new_row['specimen_id'] = row['specimen_id']
					new_row['success'] = 1
					new_row['new_taxon'] = new_taxon
				else:
					raise ValueError(str(idb['itemCount']) + ' items for specimen uuid ' + row['New_iDB_uuid'])
		else:
			new_row['specimen_id'] = row['specimen_id']
			new_row['success'] = 0
			new_row['new_taxon'] = new_taxon
		success_csv.to_csv('post_relink_success.csv', index=False, index_label=False)


if __name__ == '__main__':
	fix_specimen_links()