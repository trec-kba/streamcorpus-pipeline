'''
pipeline stage for truncating chunks at a fixed length and deleting
the overage

This software is released under an MIT/X11 open source license.

Copyright 2012-2013 Diffeo, Inc.
'''

import os
from streamcorpus import Chunk

class truncate(object):
    '''
    kba.pipeline "transform" callable of the "batch" type that takes a
    chunk and replaces with one that has up to max_items in it.    
    '''
    def __init__(self, config):
        self.config = config

    def __call__(self, chunk_path):
        '''
        batch-type transform stage: reads a chunk from chunk_path, and
        replaces it with a new chunk at the same path
        '''
        ## make a new output chunk at a temporary path
        tmp_chunk_path = chunk_path + '_'
        t_chunk = Chunk(path=tmp_chunk_path, mode='wb')

        for num, si in enumerate(Chunk(path=chunk_path)):
            if num < self.config['max_items']:
                t_chunk.add(si)
            else:
                break

        ## flush to disk
        t_chunk.close()

        ## atomic rename new chunk file into place
        os.rename(tmp_chunk_path, chunk_path)

    def shutdown(self):
        pass
