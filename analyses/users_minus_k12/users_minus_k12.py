import csv
import zlib
import sys, os
sys.path.append(os.path.abspath(os.path.join('..', '..', 'lib')))

import db
import phpserialize

def get_dict_field(d, field):
    if field in d:
        return d[field]
    else:
        return None

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

users = db.db_execute(c, sql)

non_k12_student_users = []
k12_student_users = []
all_users = len(users)
non_k12_users_count = 0
k12_users_count = 0
etc_count = 0

for row in users:
	student = 0
	u_vars = get_dict_field(row, 'vars')
	if u_vars:
		# print u_vars
		u_vars_dict = blob_to_array(u_vars)
		if u_vars_dict:
			pref = get_dict_field(u_vars_dict, '_user_preferences')
			if pref:
				 affil = get_dict_field(pref, 'user_profile_professional_affiliation')
				 if affil:	
				 	student = 0
				 	for k, v in affil.iteritems():
				 		if v == 'Student: K-6' or v == 'Student:7-12':
				 			student = 1
 	if student == 0:
 		email = get_dict_field(row, 'email')
 		if email:
 			non_k12_users_count += 1
 			non_k12_student_users.append(email)
 	elif student == 1:
 		k12_users_count += 1
 		email = get_dict_field(row, 'email')
 		if email:
 			k12_student_users.append(email)
 	else:
 		raise ValueError('?')

with open('users_minus_k12.csv', 'wb') as myfile:
    wr = csv.writer(myfile, quoting=csv.QUOTE_ALL)
    for u in non_k12_student_users:
    	wr.writerow([u])

with open('users_k12_students.csv', 'wb') as myfile:
    wr = csv.writer(myfile, quoting=csv.QUOTE_ALL)
    for u in k12_student_users:
    	wr.writerow([u])

print "Total user count: " + str(all_users)
print "Non k12 student user count: " + str(non_k12_users_count)
print "k12 student user count: " + str(k12_users_count)
print "etc count: " + str(etc_count)
