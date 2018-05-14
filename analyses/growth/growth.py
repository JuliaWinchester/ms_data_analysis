import sys, os
sys.path.append(os.path.abspath(os.path.join('..', '..', 'lib')))

import cPickle as pickle
import csv
import db
import ms_media_file
import requests

# Import overt recordsets
overt_recordsets = []
with open('morphosource_recordsets_overt.csv', 'r') as csvfile:
	reader = csv.reader(csvfile)
	for line in reader:
		if line[2] == "Yes":
			overt_recordsets.append(line[0])

conn = db.db_conn()
c = conn.cursor()

sql = """ SELECT * FROM `ms_media_files` AS mf
		  LEFT JOIN `ms_media` AS m on mf.media_id = m.media_id
		  LEFT JOIN `ms_specimens` AS s on m.specimen_id = s.specimen_id
		  LEFT JOIN `ms_institutions` AS i on s.institution_id = i.institution_id
	"""

r = db.db_query(c, sql)

m_array = []
inst_id_dict = {
	1: [],
	11: [],
	31: [],
	50: []
}
insti_mfs = []
indiv_mfs = []

i = 0
for m in r:
	print i
	mf = ms_media_file.MsMediaFile(m)
	m_array.append(mf)

	# Institutional or individual? First, is it in the list of institution IDs?
	if mf.db_dict['i.institution_id'] in [1, 11, 31, 50]:
		insti_mfs.append(mf.db_dict['media_file_id'])
		inst_id_dict[mf.db_dict['i.institution_id']].append(mf.db_dict['media_file_id'])
	else:
		# Is it from an oVert recordset
		if mf.db_dict['uuid']:
			resp = requests.get('https://search.idigbio.org/v2/view/records/' + mf.db_dict['uuid'])
			json = resp.json()
			recordset = json['indexTerms']['recordset']
			if recordset in overt_recordsets:
				insti_mfs.append(mf.db_dict['media_file_id'])
			else:
				indiv_mfs.append(mf.db_dict['media_file_id'])
		else:
			indiv_mfs.append(mf.db_dict['media_file_id'])
	i += 1

pickle.dump(inst_id_dict, open('inst_id_dict.p', 'wb'))
pickle.dump(insti_mfs, open('insti_mfs.p', 'wb'))
pickle.dump(indiv_mfs, open('indiv_mfs.p', 'wb'))



