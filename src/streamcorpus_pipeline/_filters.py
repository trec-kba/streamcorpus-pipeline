'''
incremental transforms that only pass specific StreamItems
'''

def debug_filter(config):
    def _debug_filter(si, context):
        
        if si.stream_id in config['accept_stream_ids']:
            return si
        else:
            return None

    return _debug_filter

def exclusion_filter(config):
    def _exclusion_filter(si, context):
        
        if si.stream_id in config['excluded_stream_ids']:
            return None
        else:
            return si

    return _exclusion_filter

def filter_languages(config):
    def _filter_languages(si, context):
        if not si.body.language and config.get('allow_null_language', True):
            return si

        if si.body.language.code in config.get('included_language_codes', []):
            return si

        ## otherwise return None, which excludes the language

    return _filter_languages

def remove_raw(config):
    def _remove_raw(si, context):
        if config.get('if_clean_visible_remove_raw', False):
            if si.body.clean_visible:
                si.body.raw = ''
            ## otherwise leave body.raw unchanged

        else:
            si.body.raw = ''

        return si

    return _remove_raw

