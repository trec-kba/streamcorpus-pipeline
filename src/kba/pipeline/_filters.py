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
    def _exclustion_filter(si, context):
        
        if si.stream_id in config['excluded_stream_ids']:
            return None
        else:
            return si

    return _exclustion_filter

