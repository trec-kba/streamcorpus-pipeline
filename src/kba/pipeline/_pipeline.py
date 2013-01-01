#!/usr/bin/env python
'''
Provides a data transformation pipeline for expanding the data in
StreamItem instances from streamcorpus.Chunk files.  kba.pipeline.run
provides a command line interface to this functionality.

This software is released under an MIT/X11 open source license.

Copyright 2012 Diffeo, Inc.
'''

import os
import re
import sys
import traceback
import itertools
import streamcorpus

class DummyChunk(object):
    def add(self, stream_item):
        pass
    def close(self):
        pass

from ._logging import log_full_file

## Rather than mess with __import__(), let's just define a mapping
## from strings to the particular things that we expect to see as
## transform functions.  When we want to expose this for user-defined
## transforms, we'll figure out a better interface.
from ._transforms import Transforms

def _import_transform(name, config):
    '''
    :param name: string name of a transform in Transforms

    :param config: config dict passed into each tranform constructor

    :returns callable: that takes a StreamItem as input and returns a
    StreamItem.
    '''
    assert name.startswith('kba.pipeline.'), \
        'currently only supports transforms in kba.pipeline.*, not %s' % name
    name = name.split('.')[2]

    trans = Transforms[name](config)

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
        self._transforms = [_import_transform(name, config) 
                            for name in config['transforms']]

    def run(self, chunks=[], chunk_paths=[]):
        '''
        Operate the pipeline on chunks
        '''
        for chunk in chunks:
            self._run(chunk)

        for i_path in chunk_paths:
            i_path = i_path.strip()
            if self.config['output_type'] == 'samedir':
                assert i_path[-3:] == '.sc', repr(i_path[-3:])
                o_path = i_path[:-3] + '-%s.sc' % self.config['output_name']
                print 'creating %s' % o_path
                o_chunk = streamcorpus.Chunk(path=o_path, mode='wb')

            elif self.config['output_type'] == 'None':
                ## make no output
                o_chunk = DummyChunk()

            #elif self.config['output_type'] == 'inplace':
                ## replace the input chunks with the newly created
                ## chunks

            i_chunk = streamcorpus.Chunk(path=i_path, mode='rb')
            self._run(i_chunk, o_chunk)

    def _run(self, i_chunk, o_chunk):

        ## iterate over docs from chunks in the queue
        for si in i_chunk:
            ## operate each transform on this one StreamItem
            for transform in self._transforms:
                try:
                    si = transform(si)
                except Exception, exc:
                    #logger.warning
                    print 'Pipeline caught:'
                    print traceback.format_exc(exc)
                    print 'moving on'

                    ## hack in a logging step here so we can manually inspect
                    ## this fallback stage.
                    si.body.cleaner_log = traceback.format_exc(exc)
                    log_full_file(si, 'fallback-givingup')

            ## put the StreamItem into the output
            o_chunk.add(si)
        o_chunk.close()
