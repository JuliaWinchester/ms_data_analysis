from os import walk
from os.path import getsize, join

import csv
import zipfile

dirpath = '/nfs/images/media/morphosource/images/'

results = []

with open('zip_size.csv', 'w') as f:
	result_csv = csv.writer(f)
	result_csv.writerow(['Filename', 'Compressed size', 'Uncompressed size'])
	f.close()

for root, subdirs, files in walk(dirpath):
	for file in files:
		filepath = join(root, file)
		try:
			if zipfile.is_zipfile(filepath):
				print(filepath)
				filepath = join(root, file)
				res = [filepath, getsize(filepath)]
				with zipfile.ZipFile(filepath, 'r') as z:
					res.append(z.infolist()[0].file_size)
				with open('zip_size.csv', 'a') as f:
					result_csv = csv.writer(f)
					result_csv.writerow(res)
					f.close()	
		except Exception as e:
			continue