'''
incremental transforms that only pass specific StreamItems

This software is released under an MIT/X11 open source license.

Copyright 2012-2013 Diffeo, Inc.
'''
from __future__ import absolute_import
from streamcorpus_pipeline.stages import Configured

class debug_filter(Configured):
    config_name = 'debug_filter'
    default_config = { 'accept_stream_ids': [] }
    def __call__(self, si, context):
        if si.stream_id in self.config['accept_stream_ids']:
            return si
        else:
            return None

class exclusion_filter(Configured):
    config_name = 'exclusion_filter'
    default_config = { 'excluded_stream_ids': [] }
    def __call__(self, si, context):
        if si.stream_id in self.config['excluded_stream_ids']:
            return None
        else:
            return si

class filter_languages(Configured):
    config_name = 'filter_languages'
    default_config = { 'allow_null_language': True,
                       'included_language_codes': [] }
    def __call__(self, si, context):
        if not si.body.language and self.config['allow_null_language']:
            return si
        if si.body.language.code in self.config['included_language_codes']:
            return si
        ## otherwise return None, which excludes the language
        return None

class remove_raw(Configured):
    config_name = 'remove_raw'
    default_config = { 'if_clean_visible_remove_raw': False }
    def __call__(self, si, context):
        if self.config['if_clean_visible_remove_raw']:
            if si.body.clean_visible:
                si.body.raw = ''
            ## otherwise leave body.raw unchanged
        else:
            si.body.raw = ''

        return si

