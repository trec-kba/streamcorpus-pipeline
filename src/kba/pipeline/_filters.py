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
