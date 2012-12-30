#!/usr/bin/env python
'''
Provides a data transformation pipeline for expanding the data in
StreamItem instances from streamcorpus.Chunk files.

This software is released under an MIT/X11 open source license.

Copyright 2012 Diffeo, Inc.
'''

import os
import re
import sys
import traceback
import itertools
import streamcorpus
from ._transforms import Transforms

def _import_transform(name):
    assert name.startswith('kba.pipeline.'), \
        'currently only supports transforms in kba.pipeline.*, not %s' % name
    name = name.split('.')[2]
    trans = Transforms[name]

    ## Note that fromlist must be specified here to cause __import__()
    ## to return the right-most component of name, which in our case
    ## must be a function.  The contents of fromlist is not
    ## considered; it just cannot be empty:
    ## http://stackoverflow.com/questions/2724260/why-does-pythons-import-require-fromlist
    #trans = __import__('clean_html', fromlist=['kba.pipeline'])

    return trans

class Pipeline(object):
    '''    
    configurable pipeline for transforming StreamItem instances in
    streamcorpus.Chunk files into new Chunk files.  Requires a
    configuration dict, which is loaded from a yaml file.
    '''
    def __init__(self, config):
        assert 'kba.pipeline' in config, \
            '"kba.pipeline" missing from config: %r' % config
        config = config['kba.pipeline']
        self.config = config

        ## a list of transforms that take StreamItem instances as
        ## input and emit modified StreamItem instances
        self._transforms = map(_import_transform, config['transforms'])
        ## initialize each transform
        #self._transforms = [trans(config) for trans in _transforms]

    def run(self, chunks=[], chunk_paths=[]):
        '''
        Operate the pipeline on chunks
        '''
        for chunk in chunks:
            self._run(chunk)

        for i_path in chunk_paths:
            assert self.config['output_type'] == 'samedir'
            i_path = i_path.strip()
            assert i_path[-3:] == '.sc', repr(i_path[-3:])
            o_path = i_path[:-3] + '-%s.sc' % self.config['output_name']
            print 'creating %s' % o_path
            o_chunk = streamcorpus.Chunk(path=o_path, mode='wb')
            i_chunk = streamcorpus.Chunk(path=i_path, mode='rb')
            self._run(i_chunk, o_chunk)

    def _run(self, i_chunk, o_chunk):

        ## iterate over docs from chunks in the queue
        for si in i_chunk:
            ## operate each transform on this one StreamItem
            for transform in self._transforms:
                si = transform(si)
            ## put the StreamItem into the output
            o_chunk.add(si)
        o_chunk.close()
