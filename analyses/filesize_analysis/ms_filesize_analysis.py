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
						   cursorclass=pymysql.cursors.DictCursor)

def db_query(cursor, sql, args=None):
	if args is not None:
		args = [args]
	cursor.execute(sql, args)
	return cursor.fetchall()

conn = db_conn()
c = conn.cursor()

sql = "SELECT * FROM `ms_media_files`"

r = db_query(c, sql)

file_sizes = {}

i = 0
for row in r:
	print i
	m = ms_media_file.MsMediaFile(row)
	if hasattr(m, 'mf_info_dict'):
		if '_archive_' in m.mf_info_dict:
			file_sizes[m.mf_info_dict['_archive_']['FILENAME']] = m.mf_info_dict['_archive_']['FILESIZE']
		elif 'original' in m.mf_info_dict:
			if 'PROPERTIES' in m.mf_info_dict['original']:
				if 'filesize' in m.mf_info_dict['original']['PROPERTIES']:
					file_sizes[m.mf_info_dict['original']['FILENAME']] = m.mf_info_dict['original']['PROPERTIES']['filesize']
				else:
					print('No filesize for file ' + m.mf_info_dict['original']['FILENAME'])
			else:
				print('No properties for file ' + m.mf_info_dict['original']['FILENAME'])
		else:
			print('No original or archive found for media file id ' + str(m.db_dict['media_file_id']))
	i += 1

pickle.dump(file_sizes, open('filename_filesize_dict.p', 'wb'))
pickle.dump(file_sizes.values(), open('raw_filesize_list.p', 'wb'))



