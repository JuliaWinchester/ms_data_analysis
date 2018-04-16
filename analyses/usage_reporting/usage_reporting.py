import sys, os
sys.path.append(os.path.abspath(os.path.join('..', '..', 'lib')))

import db
import media_download_stats as stats
import pandas

def get_num_mf_derived_from(mf_id):
    conn = db.db_conn_socket()
    c = conn.cursor()
    sql = """ 
        SELECT * FROM `ms_media_files`
        WHERE `derived_from_media_file_id` = %s
    """
    return len(db.db_execute(c, sql, mf_id))

def get_num_m_derived_from(m_id):
    conn = db.db_conn_socket()
    c = conn.cursor()
    sql = """ 
        SELECT * FROM `ms_media`
        WHERE `derived_from_media_id` = %s
    """
    return len(db.db_execute(c, sql, mf_id))

conn = db.db_conn_socket()
c = conn.cursor()

sql = """ 
		SELECT * FROM `ms_media_files` AS mf
		LEFT JOIN `ms_media` AS m ON m.media_id = mf.media_id
		LEFT JOIN `ms_specimens` AS s ON s.specimen_id = m.specimen_id
        LEFT JOIN `ms_specimens_x_taxonomy` AS sxt ON sxt.specimen_id = s.specimen_id
        LEFT JOIN `ms_taxonomy_names` AS t ON t.alt_id = sxt.alt_id
		WHERE `recordset` = "bd7cfd55-bf55-46fc-878d-e6e11f574ccd" 
		"""

media = db.db_execute(c, sql)

m_stats = {}

for m in media:
    m_id = m['media_id']
    mf_id = m['media_file_id']
    if m_id not in m_stats:
        # New entry, must compile media group stats
        m_stats[m_id] = {
            'mg': m,
            'mg_stats': stats.MediaDownloadStats(None, m_id),
            'mf': {},
            'mf_stats': {}
        }
    m_stats[m_id]['mf'][mf_id] = m
    m_stats[m_id]['mf_stats'][mf_id] = stats.MediaDownloadStats(mf_id)

usage_report = pandas.DataFrame(columns=
    ['media_file_id',
    'media_file_derived_from',
    'num_media_files_derived_from_this',
    'media_group_id',
    'media_group_derived_from',
    'num_media_groups_derived_from_this',
    'specimen_id',
    'specimen_institution_code',
    'specimen_collection_code',
    'specimen_catalog_number',
    'specimen_genus',
    'specimen_species',
    'total_downloads',
    'dl_intended_use_School',
    'dl_intended_use_School_K_6',
    'dl_intended_use_School_7_12',
    'dl_intended_use_School_College_Post_Secondary',
    'dl_intended_use_School_Graduate_school',
    'dl_intended_use_Education',
    'dl_intended_use_Education_K_6',
    'dl_intended_use_Education_7_12',
    'dl_intended_use_Education_College_Post_Secondary',
    'dl_intended_use_Educaton_general',
    'dl_intended_use_Education_museums_public_outreach',
    'dl_intended_use_Personal_interest',
    'dl_intended_use_Research',
    'dl_intended_use_Commercial',
    'dl_intended_use_Art',
    'dl_intended_use_other',
    'dl_intended_use_3d_print',
    'total_download_users',
    'u_affiliation_Student',
    'u_affiliation_Student:_K-6',
    'u_affiliation_Student:7-12',
    'u_affiliation_Student:_College/Post-Secondary',
    'u_affiliation_Student:_Graduate',
    'u_affiliation_Faculty',
    'u_affiliation_Faculty:_K-6',
    'u_affiliation_Faculty:7-12',
    'u_affiliation_Faculty_College/Post-Secondary',
    'u_affiliation_Staff:_College/Post-Secondary',
    'u_affiliation_General_Educator',
    'u_affiliation_Museum',
    'u_affiliation_Museum_Curator',
    'u_affiliation_Museum_Staff',
    'u_affiliation_Librarian',
    'u_affiliation_IT',
    'u_affiliation_Private_Individual',
    'u_affiliation_Researcher',
    'u_affiliation_Private_Industry',
    'u_affiliation_Artist',
    'u_affiliation_Government',
    'u_affiliation_other',
    ])

for m_id, s in m_stats.iteritems():
    # Add media group row
    mg = s['mg']
    mg_stats = s['mg_stats']
    row = {
        'media_file_id': None,
        'media_file_derived_from': None,
        'num_media_files_derived_from_this': None,
        'media_group_id': m_id,
        'media_group_derived_from': mg['derived_from_media_id'],
        'num_media_groups_derived_from_this': get_num_m_derived_from(m_id),
        'specimen_id': mg['specimen_id'],
        'specimen_institution_code': mg['institution_code'],
        'specimen_collection_code': mg['collection_code'],
        'specimen_catalog_number': mg['catalog_number'],
        'specimen_genus': mg['genus'],
        'specimen_species': mg['species'],
        'total_downloads': mg_stats.total_downloads,
        'total_download_users': mg_stats.total_users
    }
    for use, num in mg_stats.intended_use_dict.iteritems():
        row['dl_intended_use_' + use] = num
    for demo, num in mg_stats.user_demo_dict.iteritems():
        row['u_affiliation_' + demo.replace(' ', '_')] = num

    usage_report = usage_report.append(row, ignore_index=True)

    # Add media files row
    for mf_id in s['mf'].keys():
        mf = s['mf'][mf_id]
        mf_stats = s['mf_stats'][mf_id]
        row = {
            'media_file_id': mf['media_file_id'],
            'media_file_derived_from': mf['derived_from_media_file_id'],
            'num_media_files_derived_from_this': get_num_mf_derived_from(mf_id),
            'media_group_id': mf['media_id'],
            'media_group_derived_from': mf['derived_from_media_id'],
            'num_media_groups_derived_from_this': get_num_m_derived_from(mf['media_id']),
            'specimen_id': mf['specimen_id'],
            'specimen_institution_code': mf['institution_code'],
            'specimen_collection_code': mf['collection_code'],
            'specimen_catalog_number': mf['catalog_number'],
            'specimen_genus': mf['genus'],
            'specimen_species': mf['species'],
            'total_downloads': mf_stats.total_downloads,
            'total_download_users': mf_stats.total_users
        }
        for use, num in mf_stats.intended_use_dict.iteritems():
            row['dl_intended_use_' + use] = num
        for demo, num in mf_stats.user_demo_dict.iteritems():
            row['u_affiliation_' + demo.replace(' ', '_')] = num

        usage_report = usage_report.append(row, ignore_index=True)

usage_report.to_csv('usage_report.csv', index=False, index_label=False)













