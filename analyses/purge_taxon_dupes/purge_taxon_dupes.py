import sys, os
sys.path.append(os.path.abspath(os.path.join('..', '..', 'lib')))

import cPickle as pickle
import credentials
import ms_media_file
import pandas
import phpserialize
import pymysql
import zlib
import copy

def db_conn():
	return pymysql.connect(host = credentials.db['server'],
						   user = credentials.db['username'],
						   password = credentials.db['password'],
						   db = credentials.db['db'],
						   charset = 'utf8mb4',
						   cursorclass=pymysql.cursors.DictCursor,
						   autocommit=True)

def db_query(cursor, sql, args=None):
	if args is not None and type(args) != list and type(args) != tuple:
		args = [args]
	cursor.execute(sql, args)
	return cursor.fetchall()

def relink_taxonomy(c, old_alt_id, old_taxon_id, new_alt_id, new_taxon_id):
	# Update specimens_x_taxonomy
	update_sql = """
		UPDATE ms_specimens_x_taxonomy
		SET alt_id = %s, taxon_id = %s
		WHERE alt_id = %s
	"""

	db_query(c, update_sql, [new_alt_id, new_taxon_id, old_alt_id])
	
	# Delete ms_taxonomy_names rows
	delete_names_sql = """
		DELETE FROM ms_taxonomy_names
		WHERE alt_id = %s
	"""

	db_query(c, delete_names_sql, old_alt_id)

	# Delete ms_taxonomy rows
	delete_taxon_sql = """
		DELETE FROM ms_taxonomy
		WHERE taxon_id = %s
	"""

	db_query(c, delete_taxon_sql, old_taxon_id)

conn = db_conn()
c = conn.cursor()

sql = """
	SELECT * FROM ms_taxonomy_names AS n
	LEFT JOIN ms_taxonomy AS t ON t.taxon_id = n.taxon_id
	"""

r = db_query(c, sql)

# Build initial taxon names dict
td = {}
for row in r:
	if row['genus'] not in td:
		td[row['genus']] = {}
	if row['species'] not in td[row['genus']]:
		td[row['genus']][row['species']] = {}
	if row['subspecies'] not in td[row['genus']][row['species']]:
		td[row['genus']][row['species']][row['subspecies']] = {}
	td[row['genus']][row['species']][row['subspecies']][row['alt_id']] = {
		'alt_id': row['alt_id'],
		'taxon_id': row['taxon_id'],
		'ht_family': row['ht_family'],
		'ht_order': row['ht_order'],
		'ht_class': row['ht_class'],
		'ht_phylum': row['ht_phylum'],
		'ht_kingdom': row['ht_kingdom'],
		'n_specimens': 0
	}

# Check if any taxon alt_ids have more than one taxon_id
taxon_id_dict = {}
for g_name, g_dict in td.iteritems():
	for s_name, s_dict in g_dict.iteritems():
		for ss_name, ss_dict in s_dict.iteritems():
			for alt_id, name_dict in ss_dict.iteritems():
				if name_dict['taxon_id'] not in taxon_id_dict:
					taxon_id_dict[name_dict['taxon_id']] = []
				taxon_id_dict[name_dict['taxon_id']].append(name_dict['alt_id'])

for taxon_id, alt_id_array in taxon_id_dict.iteritems():
	if len(alt_id_array) > 1:
		print('Multiple alt IDs for taxon_id: ' + str(taxon_id))
		print('Alt IDs: ' + str(alt_id_array))

td_copy = copy.deepcopy(td)
# Fill out number of specimens
for g_name, g_dict in td.iteritems():
	for s_name, s_dict in g_dict.iteritems():
		for ss_name, ss_dict in s_dict.iteritems():
			for alt_id, name_dict in ss_dict.iteritems():
				specimen_sql = """
					SELECT * FROM ms_specimens AS s
					LEFT JOIN ms_specimens_x_taxonomy AS sxt ON sxt.specimen_id = s.specimen_id
					WHERE sxt.alt_id = %s
				"""
				s = db_query(c, specimen_sql, int(alt_id))
				td_copy[g_name][s_name][ss_name][alt_id]['n_specimens'] = len(s)

# Fix shit
for g_name, g_dict in td_copy.iteritems():
		for s_name, s_dict in g_dict.iteritems():
				for ss_name, ss_dict in s_dict.iteritems():
					top_alt_id = None
					top_taxon_id = None
					top_n_specimens = None
					for alt_id, name_dict in ss_dict.iteritems():
						if top_n_specimens == None or name_dict['n_specimens'] > top_n_specimens:
							top_alt_id = alt_id
							top_taxon_id = name_dict['taxon_id']
							top_n_specimens = name_dict['n_specimens']
						if name_dict['n_specimens'] == top_n_specimens and alt_id != top_alt_id and alt_id < top_alt_id:
							top_alt_id = alt_id
							top_taxon_id = name_dict['taxon_id']
							top_n_specimens = name_dict['n_specimens']
					for alt_id, name_dict in ss_dict.iteritems():
						if top_alt_id != alt_id:
							# Fix this one
							print(str(g_name) + ' ' + str(s_name) + ' ' + str(ss_name) + ' Will fold alt_id ' + str(alt_id) + ' with ' + str(name_dict['n_specimens']) + ' into alt_id ' + str(top_alt_id) + ' with ' + str(top_n_specimens) + ' specimens')
							relink_taxonomy(c, alt_id, name_dict['taxon_id'], top_alt_id, top_taxon_id)

# Further exploration
for g_name, g_dict in td_copy.iteritems():
		for s_name, s_dict in g_dict.iteritems():
				for ss_name, ss_dict in s_dict.iteritems():
					top_alt_id = None
					top_taxon_id = None
					top_n_specimens = None
					for alt_id, name_dict in ss_dict.iteritems():
						if top_n_specimens == None or name_dict['n_specimens'] > top_n_specimens:
							top_alt_id = alt_id
							top_taxon_id = name_dict['taxon_id']
							top_n_specimens = name_dict['n_specimens']
						if name_dict['n_specimens'] == top_n_specimens and alt_id != top_alt_id and alt_id < top_alt_id:
							top_alt_id = alt_id
							top_taxon_id = name_dict['taxon_id']
							top_n_specimens = name_dict['n_specimens']
					for alt_id, name_dict in ss_dict.iteritems():
						if top_alt_id != alt_id:
							# Fix this one
							print(str(g_name) + ' ' + str(s_name) + ' ' + str(ss_name) + ' Will fold alt_id ' + str(alt_id) + ' with ' + str(name_dict['n_specimens']) + ' into alt_id ' + str(top_alt_id) + ' with ' + str(top_n_specimens) + ' specimens')
							print(g_name)
							print(s_name)
							print(ss_name)
							print(name_dict['ht_family'])
							print(name_dict['ht_order'])
							print(name_dict['ht_class'])
							print(name_dict['ht_phylum'])
							print(name_dict['ht_kingdom'])
							# relink_taxonomy(c, alt_id, name_dict['taxon_id'], top_alt_id, top_taxon_id)





