import sys, os
sys.path.append(os.path.abspath(os.path.join('..', '..', 'lib')))

import db
import ms_media_file

conn = db.db_conn_socket()
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

for m in r:
	mf = ms_media_file.MsMediaFile(m)
	m_array.append(mf)

	# Institutional or individual? First, is it in the list of institution IDs?
	if mf.db_dict['i.institutional_id'] in [1, 11, 31, 50]:
		insti_mfs.append(mf.db_dict['media_file_id'])
		inst_id_dict[mf.db_dict['i.institutional_id']].append(mf.db_dict['media_file_id'])
	else:
		# Is it from an oVert recordset


