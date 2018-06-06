import sys, os
sys.path.append(os.path.abspath(os.path.join('..', '..', 'lib')))

import db
import pandas

csv = pandas.read_csv('MorphoSource_oVert_update_3_15_18.csv')

scanner_dict = {
	'Bruker Skyscan 1173': 61,
	'Phoenix VTome|x M': 57, # But this associates with facility ID 48, Florida Museum of Natural History Herpetology

}

facility_dict = {
	'Karel F. Liem Bioimaging facility': 52,
	'Nanoscale Research Facility, University of Florida': 46
}

institution_dict = {
	
}