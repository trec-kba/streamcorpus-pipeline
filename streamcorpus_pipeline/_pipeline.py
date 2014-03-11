#!/usr/bin/env python
'''
Provides a data transformation pipeline for expanding the data in
StreamItem instances from streamcorpus.Chunk files.  streamcorpus_pipeline.run
provides a command line interface to this functionality.

This software is released under an MIT/X11 open source license.

Copyright 2012-2014 Diffeo, Inc.
'''

from __future__ import absolute_import
import atexit
import itertools
import logging
import math
import os
import re
import shutil
import signal
import sys
import time
import traceback
import uuid

try:
    import gevent
except:
    ## only load gevent if it is available :-)
    gevent = None

import streamcorpus
import yakonfig
from streamcorpus_pipeline.stages import BatchTransform
from streamcorpus_pipeline._exceptions import FailedExtraction, \
    GracefulShutdown, ConfigurationError, PipelineOutOfMemory, \
    PipelineBaseException, TransformGivingUp

logger = logging.getLogger(__name__)

class PipelineFactory(object):
    '''Factory to create `Pipeline` objects from configuration.

    Call this to get a `Pipeline` object.
    '''

    def __init__(self, registry, *args, **kwargs):
        '''Create a pipeline factory.

        :param dict config: top-level "streamcorpus_pipeline" configuration
        :param StageRegistry registry: registry of stages

        '''
        super(PipelineFactory, self).__init__(*args, **kwargs)
        self.registry = registry

    def _init_stage(self, config, name):
        return self.registry.init_stage(config[name], config)

    def _init_stages(self, config, name):
        return [self.registry.init_stage(stage, config)
                for stage in config[name]]

    def __call__(self, config):
        tmpdir = config.get('tmp_dir_path')
        tmpdir = os.path.join(tmpdir, uuid.uuid4().hex)
        if not os.path.exists(tmpdir):
            os.makedirs(tmpdir)

        return Pipeline(
            rate_log_interval=config['rate_log_interval'],
            input_item_limit=config.get('input_item_limit'),
            cleanup_tmp_files=config['cleanup_tmp_files'],
            tmp_dir_path=tmpdir,
            assert_single_source=config['assert_single_source'],
            output_max_chunk_count=config.get('output_max_chunk_count'),
            output_max_clean_visible_bytes=
                config.get('output_max_clean_visible_bytes'),
            reader=self._init_stage(config, 'reader'),
            incremental_transforms=
                self._init_stages(config, 'incremental_transforms'),
            batch_transforms=self._init_stages(config, 'batch_transforms'),
            post_batch_incremental_transforms=
                self._init_stages(config, 'post_batch_incremental_transforms'),
            writers=self._init_stages(config, 'writers'),
        )

class Pipeline(object):
    '''Pipeline for extracting data into StreamItem instances.

    The pipeline has five sets of stages.  The *reader* stage reads
    from some input source and produces a series of StreamItem objects
    out.  *Incremental transforms* take single StreamItem objects in
    and produce single StreamItem objects out.  *Batch transforms* run
    on the entire set of StreamItem objects together.  There is a
    further set of *post-batch incremental transforms* which again run
    on individual StreamItem objects.  Finally, any number of *writers*
    send output somewhere, usually a streamcorpus.Chunk file.

    '''
    def __init__(self, rate_log_interval, input_item_limit,
                 cleanup_tmp_files, tmp_dir_path, assert_single_source,
                 output_max_chunk_count, output_max_clean_visible_bytes,
                 reader, incremental_transforms, batch_transforms,
                 post_batch_incremental_transforms, writers):
        self.rate_log_interval = rate_log_interval
        self.input_item_limit = input_item_limit
        self.cleanup_tmp_files = cleanup_tmp_files
        self.tmp_dir_path = tmp_dir_path
        self.assert_single_source = assert_single_source
        self.output_max_chunk_count = output_max_chunk_count
        self.output_max_clean_visible_bytes = output_max_clean_visible_bytes

        # stages that get passed in:
        self.reader = reader
        self.incremental_transforms = incremental_transforms
        self.batch_transforms = batch_transforms
        self.pbi_stages = post_batch_incremental_transforms
        self.writers = writers

        # current Chunk output file for incremental transforms
        self.t_chunk = None  
        # context allows stages to communicate with later stages
        self.context = dict(
            i_str = None,
            data = None, 
            )
        self.work_unit = None

        # TODO: this is kind of wrong and we should make try/finally DTRT
        self._shutting_down = False
        self._cleanup_done = False
        atexit.register(self._cleanup)

    def _cleanup(self):
        '''shutdown all the stages, terminate the work_unit, remove tmp dir

        This is idempotent.
        '''
        if self._cleanup_done:
            return
        if self.work_unit:
            self.work_unit.terminate()
        for transform in self.batch_transforms:
            transform.shutdown()
        if not self.cleanup_tmp_files:
            logger.info('skipping cleanup due to config.cleanup_tmp_files=False')
            self._cleanup_done = True
            return
        try:
            if self.cleanup_tmp_files:
                logger.info('attempting rm -rf %s' % self.tmp_dir_path)
                shutil.rmtree(self.tmp_dir_path)
        except Exception, exc:
            logger.debug('trapped exception from cleaning up tmp_dir_path', exc_info=True)
        self._cleanup_done = True

    def shutdown(self, sig=None, frame=None, msg=None, exit_code=None):
        if sig:
            logger.critical('shutdown inititated by signal: %r' % sig)
        elif msg:
            logger.critical('shutdown inititated, msg: %s' % msg)
        self._shutting_down = True
        self._cleanup()
        if exit_code is None:
            if msg:
                exit_code = -1
            elif sig:
                exit_code = 128 + sig
        logger.critical('shutdown in final steps, exit_code=%d' % exit_code)
        logging.shutdown()
        sys.exit(exit_code)

    def _process_task(self, work_unit):
        '''
        returns number of stream items processed
        '''
        self.work_unit = work_unit
        i_str = work_unit.key
        start_count = work_unit.data['start_count']
        start_chunk_time = work_unit.data['start_chunk_time']
        self.run(i_str, start_count, start_chunk_time)

    def run(self, i_str, start_count=0, start_chunk_time=None):
        if start_chunk_time is None:
            start_chunk_time = time.time()

        ## the reader returns generators of StreamItems
        try:
            i_chunk = self.reader(i_str)
        except FailedExtraction, exc:
            ## means that task is invalid, reader did its best
            ## and gave up, so fail the work_unit
            logger.critical('committing failure_log on %s' % (i_str),
                            exc_info = exc)
            if self.work_unit is not None:
                self.work_unit.fail(exc)
            return 0

        ## t_path points to the currently in-progress temp chunk
        t_path = None

        ## loop over all docs in the chunk processing and cutting
        ## smaller chunks if needed

        len_clean_visible = 0
        sources = set()
        next_idx = 0

        input_item_count = 0  # how many have we input and actually done processing on?

        for si in i_chunk:
            # TODO: break out a _process_stream_item function?
            next_idx += 1

            if self._shutting_down:
                break

            ## must yield to the gevent hub to allow other
            ## things to run, in particular test_pipeline
            ## needs this to verify that shutdown works.  We
            ## import gevent here instead of above out of
            ## paranoia that the kazoo module used in
            ## ZookeeperTaskQueue may need to do a monkey
            ## patch before this gets imported:
            if gevent:
                gevent.sleep(0)

            ## skip forward until we reach start_count
            if next_idx <= start_count:
                continue

            if next_idx % self.rate_log_interval == 0:
                ## indexing is zero-based, so next_idx corresponds
                ## to length of list of SIs processed so far
                elapsed = time.time() - start_chunk_time
                if elapsed > 0:
                    rate = float(next_idx) / elapsed
                    logger.info('%d in %.1f --> %.1f per sec on (pre-partial_commit) %s' % (
                        next_idx - start_count, elapsed, rate, i_str))

            if not self.t_chunk:
                ## make a temporary chunk at a temporary path
                # (Lazy allocation after we've read an item that might get processed out to the new chunk file)
                # TODO: make this EVEN LAZIER by not opening the t_chunk until inside _run_incremental_transforms whe the first output si is ready
                t_path = os.path.join(self.tmp_dir_path, 'trec-kba-pipeline-tmp-%s' % str(uuid.uuid4()))
                self.t_chunk = streamcorpus.Chunk(path=t_path, mode='wb')
            assert self.t_chunk.message == streamcorpus.StreamItem_v0_3_0, self.t_chunk.message

            # TODO: a set of incremental transforms is equivalent to a batch transform.
            # Make the pipeline explicitly configurable as such:
            # batch_transforms: [[incr set 1], big batch op, [incr set 2], ...]
            # OR: for some list of transforms (mixed incremental and batch) pipeline can detect and batchify as needed

            ## incremental transforms populate t_chunk
            ## let the incremental transforms destroy the si by returning None
            si = self._run_incremental_transforms(si, self.incremental_transforms)

            ## insist that every chunk has only one source string
            if si:
                sources.add( si.source )
                if self.assert_single_source and len(sources) != 1:
                    raise PipelineBaseException(
                        'assert_single_source, but %r' % sources)

            if si and si.body and si.body.clean_visible:
                len_clean_visible += len(si.body.clean_visible)
                ## log binned clean_visible lengths, for quick stats estimates
                #logger.debug('len(si.body.clean_visible)=%d' % int(10 * int(math.floor(float(len(si.body.clean_visible)) / 2**10)/10)))
                logger.debug('len(si.body.clean_visible)=%d' % len(si.body.clean_visible))

            if (self.output_max_chunk_count is not None and
                len(self.t_chunk) == self.output_chunk_max_count):
                logger.warn('reached output_chunk_max_count (%d) at: %d' % (len(self.t_chunk), next_idx))
                self.t_chunk.close()
                self._intermediate_output_chunk(start_count, next_idx, sources, i_str, t_path)
                start_count = next_idx

            elif (self.output_max_clean_visible_bytes is not None and
                    len_clean_visible >= self.output_chunk_max_clean_visible_bytes):
                logger.warn('reached output_chunk_max_clean_visible_bytes (%d) at: %d' % (
                        self.output_chunk_max_clean_visible_bytes, len_clean_visible))
                len_clean_visible = 0
                self.t_chunk.close()
                self._intermediate_output_chunk(start_count, next_idx, sources, i_str, t_path)
                start_count = next_idx

            input_item_count += 1
            if ((self.input_item_limit is not None) and
                (input_item_count > self.input_item_limit)):
                break

        if self.t_chunk:
            self.t_chunk.close()
            o_paths = self._process_output_chunk(start_count, next_idx, sources, i_str, t_path)
            self.t_chunk = None
        else:
            o_paths = None

        ## set start_count and o_paths in work_unit and updated
        data = dict(start_count=next_idx, o_paths=o_paths)
        logger.debug('WorkUnit.update() data=%r', data)
        if self.work_unit is not None:
            self.work_unit.data.update(data)
            self.work_unit.update()

        # return how many stream items we processed
        return next_idx

    def _intermediate_output_chunk(self, start_count, next_idx, sources, i_str, t_path):
        '''save a chunk that is smaller than the input chunk
        '''
        o_paths = self._process_output_chunk(start_count, next_idx, sources, i_str, t_path)

        if self._shutting_down:
            return

        ## set start_count and o_paths in work_unit and updated
        data = dict(start_count=next_idx, o_paths=o_paths)
        logger.debug('WorkUnit.update() data=%r', data)
        if self.work_unit is not None:
            self.work_unit.data.update(data)
            self.work_unit.update()

        ## reset t_chunk, so we get it again
        self.t_chunk = None

        # TODO: bring this back? dropped due to not wanting to pass around start_chunk_time
        # elapsed = time.time() - start_chunk_time
        # if elapsed > 0:
        #     rate = float(next_idx) / elapsed
        #     logger.info(
        #         '%d more of %d in %.1f --> %.1f per sec on (post-partial_commit) %s',
        #         next_idx - start_count, next_idx, elapsed, rate, i_str)

        ## advance start_count for next loop
        #logger.info('advancing start_count from %d to %d', start_count, next_idx)
        

    def _process_output_chunk(self, start_count, next_idx, sources, i_str, t_path):
        '''
        for the current output chunk (which should be closed):
          1. run batch transforms
          2. run post-batch incremental transforms
          3. run 'writers' to load-out the data to files or other storage
        return list of paths that writers wrote to
        '''
        ## gather the paths as the writers run
        o_paths = []
        if len(self.t_chunk) > 0:
            ## only batch transform and load if the chunk
            ## isn't empty, which can happen when filtering
            ## with stages like "find"

            ## batch transforms act on the whole chunk in-place
            self._run_batch_transforms(t_path)

            self._maybe_run_post_batch_incremental_transforms(t_path)

            # only proceed if above transforms left us with something
            if (self.t_chunk) and (len(self.t_chunk) >= 0):
                self._run_writers(start_count, next_idx, sources, i_str, t_path, o_paths)
        return o_paths

    def _run_batch_transforms(self, chunk_path):
        for transform in self.batch_transforms:
            try:
                transform.process_path(chunk_path)
            except PipelineOutOfMemory, exc:
                logger.critical('caught PipelineOutOfMemory, so shutting down')
                self.shutdown( msg=traceback.format_exc(exc) )
            except Exception, exc:
                if self._shutting_down:
                    logger.critical('ignoring exception while shutting down', exc_info=True)
                else:
                    ## otherwise, let it bubble up and kill this process
                    logger.critical('batch transform %r failed', transform, exc_info=True)
                    raise

    def _maybe_run_post_batch_incremental_transforms(self, t_path):
        ## Run post batch incremental (pbi) transform stages.
        ## These exist because certain batch transforms have 
        ## to run before certain incremental stages.
        if self.pbi_stages:
            t_path2 = os.path.join(self.tmp_dir_path, 'trec-kba-pipeline-tmp-%s' % str(uuid.uuid1()))
            # open destination for _run_incremental_transforms to write to
            self.t_chunk = streamcorpus.Chunk(path=t_path2, mode='wb')

            input_t_chunk = streamcorpus.Chunk(path=t_path, mode='rb')
            for si in input_t_chunk:
                self._run_incremental_transforms(si, self.pbi_stages)

            self.t_chunk.close()

            os.rename(t_path2, t_path)

    def _run_writers(self, start_count, next_idx, sources, i_str, t_path, o_paths):
        ## writers put the chunk somewhere, and could delete it
        name_info = dict(
            first = start_count,
            #num and md5 computed in each writers
            source = sources.pop(),
            )

        for writer in self.writers:
            try:
                logger.debug('running %r on %r: %r', writer, i_str, name_info)
                o_path = writer(t_path, name_info, i_str)                        
            except OSError, exc:
                if exc.errno == 12:
                    logger.critical('caught OSError 12 in writer, so shutting down')
                    self.shutdown( msg=traceback.format_exc(exc) )
                else:
                    logger.critical('writer (%r, %r) failed', writer, i_str, exc_info=True)
                    raise

            logger.debug('loaded (%d, %d) of %r into %r',
                    start_count, next_idx - 1, i_str, o_path)
            if o_path:
                o_paths.append( o_path )

            if self._shutting_down:
                return

    def _run_incremental_transforms(self, si, transforms):
        '''
        Run transforms on stream item.
        Item may be discarded by some transform.
        Writes successful items out to current self.t_chunk
        Returns transformed item or None.
        '''
        ## operate each transform on this one StreamItem
        for transform in transforms:
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
                si = transform(si, context=self.context)
                if si is None:
                    return None

            except TransformGivingUp:
                ## do nothing
                logger.info('transform %r giving up on %r',
                            transform, si.stream_id)

            except Exception, exc:
                logger.critical('transform %r failed on %r from i_str=%r', 
                                transform, si.stream_id, self.context.get('i_str'),
                                exc_info=True)

        assert si is not None

        ## expect to always have a stream_time
        if not si.stream_time:
            msg = 'empty stream_time: %s' % si
            logger.critical(msg)
            sys.exit(msg)

        if si.stream_id is None:
            logger.critical('empty stream_id: %r', si)
            return None

        ## put the StreamItem into the output
        assert type(si) == streamcorpus.StreamItem_v0_3_0, type(si)
        self.t_chunk.add(si)

        return si
