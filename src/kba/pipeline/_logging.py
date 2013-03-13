#!/usr/bin/env python
'''
Logging utilities for the kba.pipeline

This software is released under an MIT/X11 open source license.

Copyright 2012 Diffeo, Inc.
'''

import os
import streamcorpus

def log_full_file(stream_item, extension, log_path=None):
    '''
    Saves a full stream_item to a one-file-long chunk with a name
    'extension' added to help explain why it was saved.
    '''
    if log_path and stream_item and extension:
        fname = '%s-%s.sc' % (stream_item.stream_id, extension)
        fpath = os.path.join(log_path, fname)
        if os.path.exists(fpath):
            os.remove(fpath)
        l_chunk = streamcorpus.Chunk(path=fpath, mode='wb')
        l_chunk.add(stream_item)
        l_chunk.close()

        print 'created %s' % fpath
