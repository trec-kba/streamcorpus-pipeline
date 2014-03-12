'''
incremental transform that detects language and stores it in
StreamItem.body.language

This software is released under an MIT/X11 open source license.

Copyright 2012-2014 Diffeo, Inc.
'''

from __future__ import absolute_import
import cld
from streamcorpus import Language
from streamcorpus_pipeline.stages import Configured

class language(Configured):
    '''
    If available, use .body.raw to guess the language name/code and
    store in .body.language
    '''
    config_name = 'language'
    def __call__(self, si, context):
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