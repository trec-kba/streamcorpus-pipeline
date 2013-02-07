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
import uuid
#import gevent
import traceback
import itertools
import streamcorpus

from ._logging import log_full_file
from ._stages import _init_stage
from . import _exceptions

class Pipeline(object):
    '''    
    configurable pipeline for extracting data into StreamItem
    instances, transforming them, and creating streamcorpus.Chunk
    files.  Requires a config dict, which is loaded from a yaml file.
    '''
    def __init__(self, config):
        assert 'kba.pipeline' in config, \
            '"kba.pipeline" missing from config: %r' % config
        config = config['kba.pipeline']
        self.config = config

        ## resolve the tmp dir and make sure it exists
        if not config['tmp_dir'].startswith('/'):
            config['tmp_dir'] = os.path.join(os.getcwd(), config['tmp_dir'])

        if not os.path.exists(config['tmp_dir']):
            os.makedirs(config['tmp_dir'])

        ## load the one task queue
        task_queue_name = config['task_queue']
        self._task_queue = _init_stage(
            task_queue_name,
            config.get(task_queue_name, {}))

        ## load the one extractor
        extractor_name = config['extractor']
        self._extractor = _init_stage(
            extractor_name,
            config.get(extractor_name, {}))

        ## a list of transforms that take StreamItem instances as
        ## input and emit modified StreamItem instances
        self._incremental_transforms = [
            _init_stage(name, config.get(name, {}))
            for name in config['incremental_transforms']]

        ## a list of transforms that take a chunk path as input and
        ## return a path to a new chunk
        self._batch_transforms = [
            _init_stage(name, config.get(name, {}))
            for name in config['batch_transforms']]

        ## a list of transforms that take a chunk path as input and
        ## return a path to a new chunk
        self._loaders  = [
            _init_stage(name, config.get(name, {}))
            for name in config['loaders']]

    def run(self):
        '''
        Operate the pipeline on chunks loaded from chunk_paths
        '''
        ## keep track of the number of StreamItems seen so far by this
        ## particular instance of the pipeline, so it can be used in
        ## naming output files.  This is really only useful for
        ## single-process jobs that might be used for special corpora.
        first_stream_item_num = 0
        self.next_stream_item_num = 0

        ## iterate over input strings from the specified task_queue
        for i_str in self._task_queue:

            ## the extractor generates generators of StreamItems
            for i_chunk in self._extractor(i_str):

                ## make a temporary chunk at a temporary path
                t_path = os.path.join(self.config['tmp_dir'], 'tmp-%s' % str(uuid.uuid1()))
                t_chunk = streamcorpus.Chunk(path=t_path, mode='wb')

                ## incremental transforms populate the temporary chunk
                self._run_incremental_transforms(i_chunk, t_chunk)

                ## insist that every chunk has only one source string
                assert len(self.sources) == 1, self.sources

                ## batch transforms act on the whole chunk in-place
                self._run_batch_transforms(t_path)

                ## loaders put the chunk somewhere, or could delete it
                name_info = dict(
                    first = first_stream_item_num,
                    num = len(t_chunk),
                    #md5 computed in the loaders
                    source = self.sources.pop(),
                    )

                print('loading %r' % i_str)
                sys.stdout.flush()

                for loader in self._loaders:
                    loader(t_path, name_info, i_str)

                ## increment the first_stream_item_num to the next one in
                ## the stream
                first_stream_item_num += self.next_stream_item_num

    def _run_batch_transforms(self, chunk_path):
        for transform in self._batch_transforms:
            transform(chunk_path)

    def _run_incremental_transforms(self, i_chunk, t_chunk):
        ## iterate over docs from a chunk
        self.sources = set()
        for si in i_chunk:

            ## operate each transform on this one StreamItem
            for transform in self._incremental_transforms:
                #timer = gevent.Timeout.start_new(1)
                #thread = gevent.spawn(transform, si)
                #try:
                #    si = thread.get(timeout=timer)
                ### The approach above to timeouts did not work,
                ### because when the re module hangs in a thread, it
                ### never yields to the greenlet hub.  The only robust
                ### way to implement timeouts is with child processes,
                ### possibly via multiprocessing.  Another benefit of
                ### child processes is armoring against segfaulting in
                ### various libraries.  This probably means that each
                ### transform should implement its own timeouts.
                try:
                    si = transform(si)

                except _exceptions.TransformGivingUp:
                    ## do nothing
                    print 'transform giving up on %r' % si.stream_id
                    pass

                except Exception, exc:
                    #logger.warning
                    print 'Pipeline caught:'
                    print traceback.format_exc(exc)
                    print 'moving on'

                    if self.config['embedded_logs']:
                        si.body.logs.append( traceback.format_exc(exc) )

                    if self.config['log_dir']:
                        log_full_file(si, 'fallback-givingup', self.config['log_dir'])

            sys.stdout.flush()

            ## expect to always have a stream_time
            assert si.stream_time, si

            ## put the StreamItem into the output
            t_chunk.add(si)

            self.sources.add( si.source )

            ## track position in the stream
            self.next_stream_item_num += 1

        t_chunk.close()
