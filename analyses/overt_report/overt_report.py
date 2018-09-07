import sys, os
sys.path.append(os.path.abspath(os.path.join('..', '..', 'lib')))

import db
import pandas
import media_download_stats

overt_report = pandas.DataFrame(columns=[
	'providerManagedID',
	'derivedFromProviderManagedID',
	'IDOfContainingCollection',
	'derivedFromIDOfContainingCollection',
	'dcterms:identifier',
	'doi',
	'media_group_title',
	'locationCreated',
	'captureDevice',
	'dc:creator',
	'ms:scanningTechnician',
	'fundingAttribution',
	'specimen_id',
	'associatedSpecimenReference',
	'coreid',
	'specimen_institution_code',
	'specimen_collection_code',
	'specimen_catalog_number',
	'specimen_genus',
	'specimen_external_genus',
	'specimen_species',
	'media_group_views',
	'specimen_views',
	'total_downloads',
	'research_downloads',
	'non_research_downloads',
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

conn = db.db_conn()
c = conn.cursor()

# sql = """
# 	SELECT * FROM `ms_media_files` AS mf
# 	LEFT JOIN `ms_media` AS m ON m.media_id = mf.media_id
# 	LEFT JOIN `ms_specimens` AS s ON s.specimen_id = m.specimen_id
# 	LEFT JOIN `ms_specimens_x_taxonomy` AS sxt ON sxt.specimen_id = s.specimen_id
# 	LEFT JOIN `ms_taxonomy_names` AS tn ON tn.alt_id = sxt.alt_id
# 	LEFT JOIN `ca_users` AS u ON u.user_id = mf.user_id
# 	LEFT JOIN `ms_scanners` AS sc ON sc.scanner_id = m.scanner_id
# 	LEFT JOIN `ms_facilities` AS f ON f.facility_id = m.facility_id
# 	WHERE m.grant_support COLLATE UTF8_GENERAL_CI LIKE '%overt%'
# 	OR m.grant_support COLLATE UTF8_GENERAL_CI LIKE '%1701714%'
# 	OR m.grant_support COLLATE UTF8_GENERAL_CI LIKE '%1701943%'
# 	OR m.grant_support COLLATE UTF8_GENERAL_CI LIKE '%1701870%'
# 	OR m.grant_support COLLATE UTF8_GENERAL_CI LIKE '%1700908%'
# 	OR m.grant_support COLLATE UTF8_GENERAL_CI LIKE '%1702421%'
# 	OR m.grant_support COLLATE UTF8_GENERAL_CI LIKE '%1702263%'
# 	OR m.grant_support COLLATE UTF8_GENERAL_CI LIKE '%1701402%'
# 	OR m.grant_support COLLATE UTF8_GENERAL_CI LIKE '%1702442%'
# 	OR m.grant_support COLLATE UTF8_GENERAL_CI LIKE '%1701797%'
# 	OR m.grant_support COLLATE UTF8_GENERAL_CI LIKE '%1701737%'
# 	OR m.grant_support COLLATE UTF8_GENERAL_CI LIKE '%1701932%'
# 	OR m.grant_support COLLATE UTF8_GENERAL_CI LIKE '%1701713%'
# 	OR m.grant_support COLLATE UTF8_GENERAL_CI LIKE '%1701516%'
# 	OR m.grant_support COLLATE UTF8_GENERAL_CI LIKE '%1701665%'
# 	OR m.grant_support COLLATE UTF8_GENERAL_CI LIKE '%1702143%'
# 	OR m.grant_support COLLATE UTF8_GENERAL_CI LIKE '%1701769%'
# 	"""

sql = """
	SELECT * FROM `ms_media_files` AS mf
	LEFT JOIN `ms_media` AS m ON m.media_id = mf.media_id
	LEFT JOIN `ms_specimens` AS s ON s.specimen_id = m.specimen_id
	LEFT JOIN `ms_specimens_x_taxonomy` AS sxt ON sxt.specimen_id = s.specimen_id
	LEFT JOIN `ms_taxonomy_names` AS tn ON tn.alt_id = sxt.alt_id
	LEFT JOIN `ca_users` AS u ON u.user_id = mf.user_id
	LEFT JOIN `ms_scanners` AS sc ON sc.scanner_id = m.scanner_id
	LEFT JOIN `ms_facilities` AS f ON f.facility_id = m.facility_id
	"""

media_files = db.db_execute(c, sql)
mf_n = len(media_files)
mf_i = 1

for mf in media_files:
	print(str(mf_i) + '/' + str(mf_n))
	mf_i += 1
	mf['external_genus'] = ''
	mf['specimen_views'] = 0
	mf['media_group_views'] = 0
	mf['media_file_views'] = 0

	if 'specimen_id' in mf and mf['specimen_id'] is not None:
		# Look up resolved taxonomy names
		eg_sql = """
			SELECT * FROM `ms_specimens_x_resolved_taxonomy` AS sxrt
			LEFT JOIN `ms_resolved_taxonomy` AS rt ON rt.taxon_id = sxrt.taxon_id
			WHERE rt.rank = 'genus' AND sxrt.specimen_id = %s;
		"""

		external_genus = db.db_execute(c, eg_sql, mf['specimen_id'])

		if len(external_genus) > 1:
			print external_genus
			raise ValueError('Specimen number ' + str(mf['specimen_id']) + ' has more than one external genus')

		for g in external_genus:
			mf['external_genus'] = g['name']

		# Look up specimen views
		specimen_views_sql = """
			SELECT * FROM `ms_specimen_view_stats`
			WHERE specimen_id = %s
		"""

		specimen_views = db.db_execute(c, specimen_views_sql, mf['specimen_id'])

		mf['specimen_views'] = len(specimen_views)

	# Look up media group views 
	media_views_sql = """
		SELECT * FROM `ms_media_view_stats`
		WHERE media_id = %s
	"""

	media_views = db.db_execute(c, media_views_sql, mf['media_id'])

	mf['media_group_views'] = len(media_views)

	# Look up media file downloads
	mf_dl_sql = """
		SELECT * FROM `ms_media_download_stats` AS s
		LEFT JOIN `ca_users` AS u ON u.user_id = s.user_id
		WHERE s.media_file_id = %s
	"""

	mf_dl = db.db_execute(c, mf_dl_sql, mf['media_file_id'])

	mf_stats = media_download_stats.MediaDownloadStats(mf_dl)

	# Construct dataframe row
	row = {
		'providerManagedID': mf['media_file_id'],
		'derivedFromProviderManagedID': mf['derived_from_media_file_id'],
		'IDOfContainingCollection': mf['media_id'],
		'derivedFromIDOfContainingCollection': mf['derived_from_media_id'],
		'dcterms:identifier': mf['ark'],
		'doi': mf['doi'],
		'media_group_title': mf['title'],
		'locationCreated': mf['f.name'],
		'captureDevice': mf['name'],
		'dc:creator': mf['fname'] + " " + mf['lname'] + " <" + mf['email'] + ">",
		'ms:scanningTechnician': mf['scanner_technicians'],
		'fundingAttribution': mf['grant_support'],
		'specimen_id': mf['specimen_id'],
		'associatedSpecimenReference': mf['uuid'],
		'coreid': mf['occurrence_id'],
		'specimen_institution_code': mf['institution_code'],
		'specimen_collection_code': mf['collection_code'],
		'specimen_catalog_number': mf['catalog_number'],
		'specimen_genus': mf['genus'],
		'specimen_external_genus': mf['external_genus'],
		'specimen_species': mf['species'],
		'media_group_views': mf['media_group_views'],
		'specimen_views': mf['specimen_views'],
		'total_downloads': mf_stats.total_downloads
	}

	for use, num in mf_stats.intended_use_dict.iteritems():
		row['dl_intended_use_' + use] = num
		if use == 'Research':
			row['research_downloads'] = num
		elif num and use != 'other':
			if 'non_research_downloads' not in row:
				row['non_research_downloads'] = int(num)
			else:
				row['non_research_downloads'] += int(num)
	for demo, num in mf_stats.user_demo_dict.iteritems():
		row['u_affiliation_' + demo.replace(' ', '_')] = num

	overt_report = overt_report.append(row, ignore_index=True)

overt_report.to_csv('all_mf_report.csv', encoding='utf-8', index=False, index_label=False)