import datetime
import sys, os
sys.path.append(os.path.abspath(os.path.join('..', '..', 'lib')))

import db
import pandas

def approval_text(approval_code):
    if approval_code == 0:
        return 'pending'
    if approval_code == 1:
        return 'approved'
    if approval_code == 2:
        return 'denied'

conn = db.db_conn_socket()
c = conn.cursor()

sql = """ 
		SELECT * FROM `ms_media_download_requests` AS r
		LEFT JOIN `ms_media` AS m ON m.media_id = r.media_id
		LEFT JOIN `ms_specimens` AS s ON s.specimen_id = m.specimen_id
        LEFT JOIN `ca_users` AS u ON u.user_id = r.user_id
        LEFT JOIN `ms_specimens_x_taxonomy` AS sxt ON sxt.specimen_id = s.specimen_id
        LEFT JOIN `ms_taxonomy_names` AS t ON t.alt_id = sxt.alt_id
		WHERE `recordset` = "bd7cfd55-bf55-46fc-878d-e6e11f574ccd" 
		"""

requests = db.db_execute(c, sql)

request_report = pandas.DataFrame(columns=
    ['request_time',
    'media_id',
    'specimen_institution_code',
    'specimen_collection_code',
    'specimen_catalog_number',
    'specimen_genus',
    'specimen_species',
    'request_text',
    'request_user_first_name',
    'request_user_last_name',
    'request_user_email',
    'request_approval'
    ])

for r in requests:
    row = {
        'request_time': datetime.datetime.fromtimestamp(r['requested_on']).
            strftime('%Y-%m-%d %H:%M:%S'),
        'media_id': r['media_id'],
        'specimen_institution_code': r['institution_code'],
        'specimen_collection_code': r['collection_code'],
        'specimen_catalog_number': r['catalog_number'],
        'specimen_genus': r['genus'],
        'specimen_species': r['species'],
        'request_text': r['request'].encode('utf-8'),
        'request_user_first_name': r['fname'].encode('utf-8'),
        'request_user_last_name': r['lname'].encode('utf-8'),
        'request_user_email': r['email'],
        'request_approval': approval_text(r['status'])
    }
    request_report = request_report.append(row, ignore_index=True)

request_report.to_csv('request_report.csv', index=False, index_label=False)













