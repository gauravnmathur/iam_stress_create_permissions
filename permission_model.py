from collections import namedtuple

Permission = namedtuple('Permission', [
                        'subject_aui',
                        'object_aui',
                        'role_aui',
                        'requestor_aui'
                        ])
