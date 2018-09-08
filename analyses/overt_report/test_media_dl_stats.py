import sys, os
sys.path.append(os.path.abspath(os.path.join('..', '..', 'lib')))

import db
import pandas

conn = db.db_conn()
c = conn.cursor()

sql = """
	SELECT * FROM `ms_media_download_stats`
	"""

dl_stats = db.db_execute(c, sql)

media_group_dict = {}

for dl in dl_stats:
	if dl['media_id']:
		if dl['media_id'] not in media_group_dict:
			media_group_dict[dl['media_id']] = {
				'mg_dl': 0,
				'mf_dl_dict': {}
			}
		media_group_dict[dl['media_id']]['mg_dl'] += 1
	if dl['media_file_id'] and dl['media_id']:
		if dl['media_file_id'] not in media_group_dict[dl['media_id']]['mf_dl_dict']:
			media_group_dict[dl['media_id']]['mf_dl_dict'][dl['media_file_id']] = 0
		media_group_dict[dl['media_id']]['mf_dl_dict'][dl['media_file_id']] += 1

delta = 0
for mg, mg_dict in media_group_dict.iteritems():
	mg_dl = mg_dict['mg_dl']
	mf_dl = 0
	for mf, mf_ind_dl in mg_dict['mf_dl_dict'].iteritems():
		mf_dl += mf_ind_dl
	if mg_dl != mf_dl:
		delta += abs(mg_dl - mf_dl)
		print(str(mg) + ' has different mg dl [' + str(mg_dl) + '] and mf dl [' + str(mf_dl) + ']')

print('Delta is ' + str(delta))
# Delta turns out to be 1163, the same number of ms_download_media_stats rows where media_id exists but media_file_id IS NULL