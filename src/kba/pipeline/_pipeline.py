#!/usr/bin/env python
'''
Provides a data transformation pipeline for expanding the data in
StreamItem instances from streamcorpus.Chunk files.

This software is released under an MIT/X11 open source license.

Copyright 2012 Diffeo, Inc.
'''

import os
import re
import string
import traceback
import itertools
import streamcorpus

def _import_transform(name):
    from . import clean_html
    ## Note that fromlist must be specified here to cause __import__()
    ## to return the right-most component of name, which in our case
    ## must be a function.  The contents of fromlist is not
    ## considered; it just cannot be empty:
    ## http://stackoverflow.com/questions/2724260/why-does-pythons-import-require-fromlist
    trans = __import__(name, fromlist=[None])
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

        ## a list of transforms that take StreamItem instances as
        ## input and emit modified StreamItem instances
        self._transforms = map(_import_transform, config['transforms'])
        ## initialize each transform
        self._transforms = [trans(config) for trans in self._transforms]

    def run(self, chunks=[], chunk_paths=[]):
        '''
        Operate the pipeline on chunks
        '''
        for chunk in chunks:
            self._run(chunk)

        for chunk_path in chunk_paths:
            self._run(streamcorpus.Chunk(path=chunk_path, mode='rb'))

    def _run(self, chunk):
        ## iterate over docs from chunks in the queue
        for si in chunk:
            ## operate each transform on this one StreamItem
            for transform in self._transforms:
                si = transform(si)
            ## put the StreamItem into the output
            o_chunk.add(si)
        o_chunk.close()

    def add_transform(self, transform):
        self._transforms.append(transform)

if __name__ == '__main__':
    import yaml
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('config', metavar='config.yaml', 
                        help='configuration parameters for a pipeline run')
    args = parser.parse_args()

    assert os.path.exists(args.config), '%s does not exist' % args.config
    config = yaml.load(open(args.config))

    pipeline = Pipeline(config)
    pipeline.run()
