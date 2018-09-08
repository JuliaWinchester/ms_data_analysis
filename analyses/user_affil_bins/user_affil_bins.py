import sys, os
sys.path.append(os.path.abspath(os.path.join('..', '..', 'lib')))

import csv
import db
import phpserialize

def get_json_field(json, field):
	if type(field) is list or type(field) is tuple:
		if field[0] in json:
			new_field = field[1:]
			if len(new_field) == 1:
				new_field = new_field[0]	
			return get_json_field(json[field[0]], new_field)
		else:
			return ''
	else:
		if field in json:
			return json[field]
		else:
			return ''

def get_dict_field(d, field):
    if field in d:
        return d[field]
    else:
        return None

def get_db_field(row, field):
	if field in row:
		return str(row[field]).lower()
	else:
		return ''

def blob_to_array(blob):
    try:
        return phpserialize.unserialize(zlib.decompress(blob))
    except:
        return phpserialize.unserialize(blob.decode('base64'))

conn = db.db_conn()
c = conn.cursor()
sql = """ 
		SELECT * FROM ca_users
	 """

r = db.db_execute(c, sql)

users_field_dict = {
	'anthropology': {
		'full': 0,
		'download': 0
	},
	'anthropological': {
		'full': 0,
		'download': 0
	},
	'biology': {
		'full': 0,
		'download': 0
	},
	'biomedical': {
		'full': 0,
		'download': 0
	},
	'biological': {
		'full': 0,
		'download': 0
	},
	'anatomy': {
		'full': 0,
		'download': 0
	},
	'anatomical': {
		'full': 0,
		'download': 0
	},
	'geology': {
		'full': 0,
		'download': 0
	},
	'geological': {
		'full': 0,
		'download': 0
	},
	'medical': {
		'full': 0,
		'download': 0
	},
	'paleontology': {
		'full': 0,
		'download': 0
	},
	'paleontological': {
		'full': 0,
		'download': 0
	},
	'paleobiology': {
		'full': 0,
		'download': 0
	},
	'natural history': {
		'full': 0,
		'download': 0
	},
	'entomology': {
		'full': 0,
		'download': 0
	},
	'icthyology': {
		'full': 0,
		'download': 0
	},
	'mammalogy': {
		'full': 0,
		'download': 0
	},
	'herpetology': {
		'full': 0,
		'download': 0
	},
	'zoology': {
		'full': 0,
		'download': 0
	}
}

# ['_user_preferences']['user_profile_organization']
# ['_user_preferences']['user_profile_address1']
# ['_user_preferences']['user_profile_address2']
for u in r:
	single_user_field_dict = {
		'anthropology': 0,
		'anthropological': 0,
		'biology': 0,
		'biomedical': 0,
		'biological': 0,
		'anatomy': 0,
		'anatomical': 0,
		'geology': 0,
		'geological': 0,
		'medical': 0,
		'paleontology': 0,
		'paleontological': 0,
		'paleobiology': 0,
		'natural history': 0,
		'entomology': 0,
		'icthyology': 0,
		'mammalogy': 0,
		'herpetology': 0,
		'zoology': 0
	}
	u_vars = get_dict_field(u, 'vars')
	if u_vars:
		u_vars_blob = blob_to_array(u_vars)
		if u_vars_blob and '_user_preferences' in u_vars_blob:
			prefs = u_vars_blob['_user_preferences']

			for f in single_user_field_dict.keys():
				if 'user_profile_organization' in prefs and f in prefs['user_profile_organization'].lower():
					single_user_field_dict[f] = 1
				if 'user_profile_address1' in prefs and f in prefs['user_profile_address1'].lower():
					single_user_field_dict[f] = 1 
				if 'user_profile_address2' in prefs and f in prefs['user_profile_address2'].lower():
					single_user_field_dict[f] = 1
		
			for f, b in single_user_field_dict.iteritems():
				if b:
					u_class = get_dict_field(u, 'userclass')
					if u_class == 1:
						users_field_dict[f]['full'] += 1
					elif u_class == 50 or u_class == 100:
						users_field_dict[f]['download'] += 1

with open('users_field_names.csv', 'wb') as csvfile:
	user_csv = csv.writer(csvfile)
	user_csv.writerow(['Field', 'Full access', 'Download only'])
	for f, d in users_field_dict.iteritems():
		user_csv.writerow([f, d['full'], d['download']])

		
