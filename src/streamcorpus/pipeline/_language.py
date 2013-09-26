'''
incremental transform that detects language and stores it in
StreamItem.body.language

This software is released under an MIT/X11 open source license.

Copyright 2012 Diffeo, Inc.
'''

import cld
from streamcorpus import Language

def language(config):
    '''
    If available, use .body.raw to guess the language name/code and
    store in .body.language
    '''
    def _language(si, context):
        if si.body and si.body.raw:
            name, code, is_reliable, num_text_bytes, details = cld.detect(si.body.raw)
            if is_reliable and code != 'xxx':
                si.body.language = Language(code=code, name=name)
            else:
                si.body.language = Language(code='', name='')

        elif si.body:
            ## no .body.raw -- rare, but not impossible
            si.body.language = Language(code='', name='')

        return si

    return _language
