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
	if hasattr(m, 'mf_info_dict'):
		if '_archive_' in m.mf_info_dict:
			result[row['project_id']].append(m.mf_info_dict['_archive_']['FILESIZE'])
		elif 'original' in m.mf_info_dict:
			if 'PROPERTIES' in m.mf_info_dict['original']:
				if 'filesize' in m.mf_info_dict['original']['PROPERTIES']:
					result[row['project_id']].append(m.mf_info_dict['original']['PROPERTIES']['filesize'])
				else:
					print(str(i))
					print('No filesize for file ' + m.mf_info_dict['original']['FILENAME'])
			else:
				print(str(i))
				print('No properties for file ' + m.mf_info_dict['original']['FILENAME'])
		else:
			print(str(i))
			print('No original or archive found for media file id ' + str(m.db_dict['media_file_id']))
	i += 1

for project_id, file_size_array in result.iteritems():
	sql_update = "UPDATE ms_projects SET total_storage_allocation = " + str(sum(file_size_array)) + " WHERE project_id = " + str(project_id)
	print(sql_update)
	r = db_query(c, sql_update)

