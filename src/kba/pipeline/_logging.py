


import os
import streamcorpus

def log_full_file(stream_item, extension, log_path='/data/trec-kba/data/wlc-streamcorpus-logs'):
    '''
    Saves a full stream_item to a one-file-long chunk with a name
    'extension' added to help explain why it was saved.
    '''
    fname = '%s-%s.sc' % (stream_item.stream_id, extension)
    fpath = os.path.join(log_path, fname)
    l_chunk = streamcorpus.Chunk(path=fpath, mode='wb')
    l_chunk.add(stream_item)
    l_chunk.close()

    print 'created %s' % fpath
