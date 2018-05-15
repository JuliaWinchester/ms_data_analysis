import csv
import sys, os
sys.path.append(os.path.abspath(os.path.join('..', '..', 'lib')))
import time
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
			warnings.warn('Returning empty string, did not find field ' + str(field[0]) + 'in json ' + str(json))
			return ''
	else:
		if field in json:
			return json[field]
		else:
			warnings.warn('Returning empty string, did not find field ' + str(field) + 'in json ' + str(json))
			return ''

def get_db_field(row, field):
	if field in row:
		return str(row[field]).lower()
	else:
		warnings.warn('Returning empty string, did not find field ' + str(field) + 'in db row ' + str(row))
		return ''

def fix_specimen_links():
	success_csv = pandas.DataFrame(columns=
		['specimen_id',
		'success',
		'new_taxon'		
		])
	#success_csv = pandas.read_csv('post_relink_success.csv')

	specimen_csv = csv_df = pandas.read_csv('specimens_to_be_fixed.csv')

	for index, row in specimen_csv.iterrows():
		success_csv = pandas.read_csv('fix_success_summary.csv')
		new_row = {}
		new_taxon = 0
		print(row['specimen_id'])
		if row['New_iDB_uuid']:
			# Get MS specimen record
			conn = db.db_conn_socket()
			c = conn.cursor()
			sql = """
				SELECT * FROM `ms_specimens` AS s
				JOIN `ms_specimens_x_taxonomy` AS sxt ON sxt.specimen_id = s.specimen_id
				JOIN `ms_taxonomy_names` AS n ON n.alt_id = sxt.alt_id
				WHERE s.specimen_id = %s
				"""
			spec_rec = db.db_execute(c, sql, row['specimen_id'])
			spec_rec = spec_rec[0]

			# Get iDB specimen record for new UUID
			idb = requests.get(
				'https://search.idigbio.org/v2/view/records/' +
				row['New_iDB_uuid']).json()
			item = idb
			
			# ------------------TAXONOMY RECORD-------------------------
			# Do taxa differ?
			if get_json_field(item, ['indexTerms', 'genus']).lower() != spec_rec['genus'].lower():
				print('Current taxon and real taxon differ')
				print(get_json_field(item, ['indexTerms', 'genus']).lower())
				print(spec_rec['genus'].lower())
				# Taxa differ. Is there a MS taxon for the 'new' taxon?
				conn = db.db_conn_socket()
				c = conn.cursor()
				sql = """
					SELECT * FROM `ms_taxonomy` AS t
					JOIN `ms_taxonomy_names` AS n ON n.taxon_id = t.taxon_id
					WHERE n.genus = %s AND n.species = %s
					"""
				r = db.db_execute(c, sql, [
					get_json_field(item, ['indexTerms', 'genus']),
					get_json_field(item, ['indexTerms', 'specificepithet'])
					])

				if len(r) == 0:
					print('Need to generate new taxon')
					# Need to generate an all new taxon
					'''
					Steps:
						1) Delete the sxt link to the current taxon
						2) Create a new ms_taxonomy record for this taxon
						3) Create a new ms_taxonomy_names record for this taxon, using iDB
						4) Create a new sxt link from specimen to new taxon
					'''

					# Get old taxon_id
					conn = db.db_conn_socket()
					c = conn.cursor()
					sql = """
						SELECT * FROM `ms_specimens_x_taxonomy` 
						WHERE specimen_id = %s
						"""
					old_sxt = db.db_execute(c, sql, int(row['specimen_id']))

					if len(old_sxt) == 1:
						old_taxon_id = old_sxt[0]['taxon_id']
						old_link_id = old_sxt[0]['link_id']
					else:
						raise ValueError('More than one sxt for specimen_id ' + str(row['specimen_id']))

					# delete old link
					conn = db.db_conn_socket()
					c = conn.cursor()
					sql = """
						DELETE FROM `ms_specimens_x_taxonomy`
						WHERE link_id = %s
					"""
					del_res = db.db_execute(c, sql, int(old_link_id))

					# create new ms_taxonomy_record
					taxon_vals = [int(spec_rec['project_id']), int(spec_rec['user_id']), int(time.time()), int(time.time())]

					conn = db.db_conn_socket()
					c = conn.cursor()
					sql = """
						INSERT INTO `ms_taxonomy`
						(project_id, user_id, created_on, last_modified_on)
						VALUES
						(%s, %s, %s, %s)
					"""
					tax_res = db.db_execute(c, sql, taxon_vals)

					# create new ms_taxonomy_names
					new_taxon_id = c.lastrowid

					tn_vals = [
						int(spec_rec['project_id']),
						int(new_taxon_id),
						int(spec_rec['user_id']),
						get_json_field(item, ['indexTerms', 'specificepithet']),
						get_json_field(item, ['indexTerms', 'kingdom']),
						get_json_field(item, ['indexTerms', 'phylum']),
						get_json_field(item, ['indexTerms', 'class']),
						get_json_field(item, ['indexTerms', 'order']),
						get_json_field(item, ['indexTerms', 'family']),
						int(time.time()),
						int(time.time()),
						get_json_field(item, ['indexTerms', 'genus']).capitalize()
					]

					conn = db.db_conn_socket()
					c = conn.cursor()
					sql = """
						INSERT INTO `ms_taxonomy_names`
						(project_id, taxon_id, user_id, species, ht_kingdom, ht_phylum, ht_class, ht_order, ht_family, created_on, last_modified_on, genus, is_primary)
						VALUES
						(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 1)
					"""
					tn_res = db.db_execute(c, sql, tn_vals)

					# create new sxt
					new_alt_id = c.lastrowid

					conn = db.db_conn_socket()
					c = conn.cursor()
					sql = """
						INSERT INTO `ms_specimens_x_taxonomy`
						(specimen_id, taxon_id, alt_id, user_id)
						VALUES
						(%s, %s, %s, %s)
					"""
					ins_res = db.db_execute(c, sql, [int(row['specimen_id']), int(new_taxon_id), int(new_alt_id), int(spec_rec['user_id'])])

					new_taxon = 1

				elif len(r) == 1:
					print('Matching specimen to currently existing taxon record')
					# Need to associate this specimen with currently existing new taxon
					'''
					Steps:
						1) Get old taxon id
						2) Get new taxon id
						3) Check for previously existing link between specimen to new_taxon_id
						4) delete old link
						5) create new link
					'''

					# Get old taxon_id
					conn = db.db_conn_socket()
					c = conn.cursor()
					sql = """
						SELECT * FROM `ms_specimens_x_taxonomy` 
						WHERE specimen_id = %s
						"""
					old_sxt = db.db_execute(c, sql, [row['specimen_id']])

					if len(old_sxt) == 1:
						old_taxon_id = int(old_sxt[0]['taxon_id'])
						old_link_id = int(old_sxt[0]['link_id'])
					else:
						raise ValueError('More than one sxt for specimen_id ' + row['specimen_id'])

					# Get new taxon_id
					new_taxon_id = int(r[0]['taxon_id'])
					new_alt_id = int(r[0]['alt_id'])

					# Is there already an sxt for the new_taxon_id?
					conn = db.db_conn_socket()
					c = conn.cursor()
					sql = """
						SELECT * FROM `ms_specimens_x_taxonomy`
						WHERE specimen_id = %s AND taxon_id = %s
						"""
					new_sxt = db.db_execute(c, sql, [int(row['specimen_id']), new_taxon_id])

					if len(new_sxt) > 0:
						raise ValueError('Already existing link between taxon id ' + str(new_taxon_id) + ' and specimen_id ' + str(row['specimen_id']))
					else:
						#delete old sxt
						conn = db.db_conn_socket()
						c = conn.cursor()
						sql = """
							DELETE FROM `ms_specimens_x_taxonomy`
							WHERE link_id = %s
						"""
						del_res = db.db_execute(c, sql, old_link_id)

						#create new sxt
						conn = db.db_conn_socket()
						c = conn.cursor()
						sql = """
							INSERT INTO `ms_specimens_x_taxonomy`
							(specimen_id, taxon_id, alt_id, user_id)
							VALUES
							(%s, %s, %s, %s)
						"""
						ins_res = db.db_execute(c, sql, [int(row['specimen_id']), new_taxon_id, new_alt_id, int(spec_rec['user_id'])])

						new_taxon = 1
				else:
					raise ValueError(str(len(r)) + ' MS taxon records for specimen uuid ' + str(row['New_iDB_uuid']))

			# -------------------SPECIMEN RECORD------------------------
			# Copy over specimen record values from iDB
			new_inst_code = get_json_field(item, ['indexTerms', 'institutioncode']).upper()
			new_coll_code = get_json_field(item, ['indexTerms', 'collectioncode'])
			new_catalog_num = get_json_field(item, ['indexTerms', 'catalognumber'])
			new_uuid = get_json_field(item, ['indexTerms', 'uuid'])
			new_recordset = get_json_field(item, ['indexTerms', 'recordset'])
			val_array = [new_inst_code, new_coll_code, new_catalog_num, new_uuid, new_recordset, int(row['specimen_id'])]

			conn = db.db_conn_socket()
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
			new_row['specimen_id'] = row['specimen_id']
			new_row['success'] = 0
			new_row['new_taxon'] = new_taxon
		success_csv = success_csv.append(new_row, ignore_index=True)
		success_csv.to_csv('fix_success_summary.csv', index=False, index_label=False)


if __name__ == '__main__':
	fix_specimen_links()