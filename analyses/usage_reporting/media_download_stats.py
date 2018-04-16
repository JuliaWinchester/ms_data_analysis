import zlib
import sys, os
sys.path.append(os.path.abspath(os.path.join('..', '..', 'lib')))

import db
import phpserialize


class MediaDownloadStats:
    '''This class defines a MorphoSource media file/group stats object.
    Class takes in an array of database dicts with each database dict
    representing a single database download stats row joined with a user row.
    These are then compiled to create summary usage stats for a single media
    file/group.
    '''
    def __init__(self, media_file_id=None, media_id=None):
        self.media_file_id = None
        self.media_id = None
        self.db_dict_array = []
        self.total_downloads = 0
        self.intended_use_dict = {
            'School': 0,
            'School_K_6': 0,
            'School_7_12': 0,
            'School_College_Post_Secondary': 0,
            'School_Graduate_school': 0,
            'Education': 0,
            'Education_K_6': 0,
            'Education_7_12': 0,
            'Education_College_Post_Secondary': 0,
            'Educaton_general': 0,
            'Education_museums_public_outreach': 0,
            'Personal_interest': 0,
            'Research': 0,
            'Commercial': 0,
            'Art': 0,
            'other': [],
            '3d_print': 0
        }
        self.total_users = 0
        self.user_demo_dict = {
            'Student': 0,
            'Student: K-6': 0,
            'Student:7-12': 0,
            'Student: College/Post-Secondary': 0,
            'Student: Graduate': 0,
            'Faculty': 0,
            'Faculty: K-6': 0,
            'Faculty:7-12': 0,
            'Faculty College/Post-Secondary': 0,
            'Staff: College/Post-Secondary': 0,
            'General Educator': 0,
            'Museum': 0,
            'Museum Curator': 0,
            'Museum Staff': 0,
            'Librarian': 0,
            'IT': 0,
            'Private Individual': 0,
            'Researcher': 0,
            'Private Industry': 0,
            'Artist': 0,
            'Government': 0,
            'other': []
        }

        self.media_file_id = media_file_id
        self.media_id = media_id
        self.get_db_dict_array()
        self.build_download_stats()
        self.build_user_demographics()

    def get_dict_field(self, d, field):
        if field in d:
            return d[field]
        else:
            return None

    def blob_to_array(self, blob):
        try:
            return phpserialize.unserialize(zlib.decompress(blob))
        except:
            return phpserialize.unserialize(blob.decode('base64'))

    def init_db(self):
        conn = db.db_conn_socket()
        c = conn.cursor()
        return c

    def get_db_dict_array(self):
        if self.media_file_id and self.media_id is None:
            sql = """
                SELECT * FROM `ms_media_download_stats` AS s
                LEFT JOIN ca_users AS u ON u.user_id = s.user_id
                WHERE `media_file_id` = %s
            """
            args = self.media_file_id
        elif self.media_file_id is None and self.media_id:
            sql = """
                SELECT * FROM `ms_media_download_stats` AS s
                LEFT JOIN ca_users AS u ON u.user_id = s.user_id
                WHERE `media_id` = %s
            """
            args = self.media_id
        elif self.media_file_id and self.media_id:
            sql = """
                SELECT * FROM `ms_media_download_stats` AS s
                LEFT JOIN ca_users AS u ON u.user_id = s.user_id
                WHERE `media_file_id` = %s AND
                `media_id` = %s
            """
            args = [self.media_file_id, self.media_id]
        else:
            raise ValueError()

        c = self.init_db()
        self.db_dict_array = db.db_execute(c, sql, args)

    def build_download_stats(self):
        for row in self.db_dict_array:
            self.total_downloads += 1
            intended_use_blob = self.get_dict_field(row, 'intended_use')
            if intended_use_blob:
                intended_use = self.blob_to_array(intended_use_blob)
                for k, v in intended_use.iteritems():
                    if v in self.intended_use_dict:
                        self.intended_use_dict[v] += 1
            intended_use_other = self.get_dict_field(row, 'intended_use_other')
            if intended_use_other:
                self.intended_use_dict['other'].append(intended_use_other)
            print_3d = self.get_dict_field(row, '3d_print')
            if print_3d:
                self.intended_use_dict['3d_print'] += 1
        self.intended_use_dict['School'] = self.intended_use_dict['School_K_6'] + \
            self.intended_use_dict['School_7_12'] + \
            self.intended_use_dict['School_College_Post_Secondary'] + \
            self.intended_use_dict['School_Graduate_school']
        self.intended_use_dict['Education'] = self.intended_use_dict['Education_K_6'] + \
            self.intended_use_dict['Education_7_12'] + \
            self.intended_use_dict['Education_College_Post_Secondary'] + \
            self.intended_use_dict['Educaton_general'] + \
            self.intended_use_dict['Education_museums_public_outreach']
        self.intended_use_dict['other'] = "; ".join(self.intended_use_dict['other'])

    def build_user_demographics(self):
        user_dict = {}
        for row in self.db_dict_array:
            if row['u.user_id'] not in user_dict:
                user_dict[row['u.user_id']] = row
        self.total_users = len(user_dict.keys())
        for u_id, u in user_dict.iteritems():
            u_vars = self.blob_to_array(self.get_dict_field(u, 'vars'))
            pref = self.get_dict_field(u_vars, '_user_preferences')
            if pref:
                affil = self.get_dict_field(pref, 'user_profile_professional_affiliation')
                if affil:
                    for k, v in affil.iteritems():
                        self.user_demo_dict[v] += 1
                affil_other = self.get_dict_field(pref, 'user_profile_professional_affiliation_other')
                if affil_other:
                    self.user_demo_dict['other'].append(affil_other)
        self.user_demo_dict['Student'] = self.user_demo_dict['Student: K-6'] + \
            self.user_demo_dict['Student:7-12'] + \
            self.user_demo_dict['Student: College/Post-Secondary'] + \
            self.user_demo_dict['Student: Graduate']
        self.user_demo_dict['Faculty'] = self.user_demo_dict['Faculty: K-6'] + \
            self.user_demo_dict['Faculty:7-12'] + \
            self.user_demo_dict['Faculty College/Post-Secondary']
        self.user_demo_dict['Museum'] = self.user_demo_dict['Museum Curator'] + \
            self.user_demo_dict['Museum Staff']
        self.user_demo_dict['other'] = "; ".join(self.user_demo_dict['other'])