import sys, os
sys.path.append(os.path.abspath(os.path.join('..', '..', 'lib')))

import cPickle as pickle
import credentials
import ms_media_file
import pandas
import phpserialize
import pymysql
import zlib

def db_conn():
	return pymysql.connect(host = credentials.db['server'],
						   user = credentials.db['username'],
						   password = credentials.db['password'],
						   db = credentials.db['db'],
						   charset = 'utf8mb4',
						   cursorclass=pymysql.cursors.DictCursor,
						   autocommit=True)

def db_query(cursor, sql, args=None):
	if args is not None and (type(args) != list or type(args) != tuple):
		args = [args]
	cursor.execute(sql, args)
	return cursor.fetchall()

conn = db_conn()
c = conn.cursor()

sql = """
	SELECT * FROM ms_media_files AS mf
	LEFT JOIN ms_media AS m ON m.media_id = mf.media_id
	LEFT JOIN ms_projects AS p ON p.project_id = m.project_id
	"""

r = db_query(c, sql)

result = {}

r_len = len(r)
i = 0
for row in r:
	print(str(i) + '/' + str(r_len))
	if row['project_id'] not in result:
		result[row['project_id']] = []
	m = ms_media_file.MsMediaFile(row)
	storage = 0
	if hasattr(m, 'mf_info_dict') and type(m.mf_info_dict) == dict:
		for key, val in m.mf_info_dict.iteritems():
			if key == '_archive_' and val is not None and 'FILESIZE' in val:
				storage += int(val['FILESIZE'])
				# print('Adding value' + str(int(val['FILESIZE'])) + ' to storage')
			elif val is not None and 'PROPERTIES' in val and 'filesize' in val['PROPERTIES']:
				storage += int(val['PROPERTIES']['filesize'])
				# print('Adding value' + str(int(val['PROPERTIES']['filesize'])) + ' to storage')
	if storage > 0:
		result[row['project_id']].append(storage)
		# print('Appending value ' + str(storage) + ' to media ' + str(m.db_dict['media_file_id']))
	else:
		print('No filesize for file ' + m.db_dict['media_file_id'])
	i += 1

for project_id, file_size_array in result.iteritems():
	sql_update = "UPDATE ms_projects SET total_storage_allocation = " + str(sum(file_size_array)) + " WHERE project_id = " + str(project_id)
	print(sql_update)
	r = db_query(c, sql_update)

