
def truncate(config):
    '''
    returns a kba.pipeline "transform" function that populates
    body.media_type if it is empty and the content type is easily
    guessed.
    '''
    ## make a closure around config
    global count
    count = 0
    def _truncate(stream_item):
        global count
        count += 1
        if count < config['max_items']:
            return stream_item
        else:
            return None

    return _truncate
