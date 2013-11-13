#!/usr/bin/env python
'''
Provides a data transformation pipeline for expanding the data in
StreamItem instances from streamcorpus.Chunk files.  streamcorpus.pipeline.run
provides a command line interface to this functionality.

This software is released under an MIT/X11 open source license.

Copyright 2012 Diffeo, Inc.
'''

import os
import re
import sys
import time
import uuid
import math
import shutil
import atexit
import signal
import logging
import traceback
import itertools
import streamcorpus

try:
    import gevent
except:
    ## only load gevent if it is available :-)
    gevent = None

from _logging import log_full_file
from stages import _init_stage, BatchTransform
from _exceptions import FailedExtraction, GracefulShutdown, ConfigurationError
import _exceptions

logger = logging.getLogger(__name__)

class Pipeline(object):
    '''    
    configurable pipeline for extracting data into StreamItem
    instances, transforming them, and creating streamcorpus.Chunk
    files.  Requires a config dict, which is loaded from a yaml file.
    '''
    def __init__(self, config):
        assert 'streamcorpus.pipeline' in config, \
            '"streamcorpus.pipeline" missing from config: %r' % config
        config = config['streamcorpus.pipeline']
        self.config = config

        self._shutting_down = False
        self.t_chunk = None  # current Chunk output file for incremental transforms

        tmpdir = config.get('tmp_dir_path')
        if tmpdir is None:
            ## if someone wants to use /tmp, it should be explicit
            raise ConfigurationError('tmp_dir_path required parameter')
        config['tmp_dir_path'] = os.path.join(tmpdir, uuid.uuid4().hex)

        if not os.path.exists(config['tmp_dir_path']):
            try:
                os.makedirs(config['tmp_dir_path'])
            except Exception, exc:
                logger.debug('tmp_dir_path already there?', exc_info=True)
                pass

        if 'rate_log_interval' in self.config:
            self.rate_log_interval = self.config['rate_log_interval']
        else:
            self.rate_log_interval = 100

        if 'external_stages_path' in config:
            import imp
            try:
                external_stages = imp.load_source('', config['external_stages_path'])
            except Exception, exc:
                logger.critical('failed to load external_stages_path: %r' % config['external_stages_path'])
                sys.exit(traceback.format_exc(exc))
            external_stages = external_stages.Stages
        else:
            external_stages = None

        logger.critical('starting a task_queue')

        ## load the one task queue
        input_list = config.get('inputs_path')
        if input_list:
            self._task_queue = input_list
        else:
            task_queue_name = config['task_queue']
            self._task_queue = _init_stage(
                task_queue_name,
                config.get(task_queue_name, {}),
                external_stages)

        ## load the one extractor
        extractor_name = config['extractor']
        self._extractor = _init_stage(
            extractor_name,
            config.get(extractor_name, {}),
            external_stages)

        ## a list of transforms that take StreamItem instances as
        ## input and emit modified StreamItem instances
        self._incremental_transforms = [
            _init_stage(name, config.get(name, {}),
                        external_stages)
            for name in config['incremental_transforms']]

        ## a list of transforms that take a chunk path as input and
        ## return a path to a new chunk
        self._batch_transforms = []
        for name in config['batch_transforms']:
            bt = _init_stage(name, config.get(name, {}), external_stages)
            assert isinstance(bt, BatchTransform), '%s is not a BatchTransform, got %r' % (name, bt)
            self._batch_transforms.append(bt)

        ## a list of transforms that take a chunk path as input and
        ## return a path to a new chunk
        if 'post_batch_incremental_transforms' in config:
            self._pbi_stages = [
                _init_stage(name, config.get(name, {}),
                            external_stages)
                for name in config['post_batch_incremental_transforms']]
        else:
            self._pbi_stages = []

        ## a list of transforms that take a chunk path as input and
        ## return a path to a new chunk
        self._loaders  = [] 
        for name in config['loaders']:
            _loader_config = config.get(name, {})
            _loader_config['tmp_dir_path'] = config.get('tmp_dir_path')
            self._loaders.append( 
                _init_stage(name, _loader_config, external_stages))

        for sig in [signal.SIGTERM, signal.SIGABRT, signal.SIGHUP, signal.SIGINT]:
            logger.debug('setting signal handler for %r' % sig)
            signal.signal(sig, self.shutdown)

        self.work_unit = None
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
        for transform in self._batch_transforms:
            transform.shutdown()
        try:
            shutil.rmtree(self.config['tmp_dir_path'])
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


    def run(self):
        '''
        operate pipeline on chunks loaded from task_queue
        '''
        start_processing_time = time.time()

        # context passed to incremental transforms
        self.context = dict(
            i_str = None,
            data = None, 
            )

        ## iterate over input strings from the specified task_queue
        tasks = iter(self._task_queue)
        while 1:
            # Weird non-idiomatic loop would otherwise be
            # "for a,b,c in self._task_queue" except that we want
            # iterator to be able to raise GracefulShutdown.
            try:
                start_count, i_str, data = tasks.next()

                self.context['i_str'] = i_str
                self.context['data'] = data

            except StopIteration:
                break
            except GracefulShutdown, exc:
                ## do a graceful shutdown instead of just a simple
                ## clean exit, because we want to do the various
                ## .shutdown() calls, e.g. to the task_queue itself
                exit_code = 0
                self.shutdown(
                    exit_code=exit_code,
                    msg='executing %r, exit_code=%d' % (exc, exit_code))

            if self._shutting_down:
                break

            start_chunk_time = time.time()
            next_idx = self._process_task(start_count, i_str, start_chunk_time)
            ## record elapsed time
            elapsed = time.time() - start_chunk_time
            if elapsed > 0:
                rate = float(next_idx) / elapsed
                logger.info('%d in %.1f sec --> %.1f per sec on (completed) %s' % (
                        next_idx, elapsed, rate, i_str))

            ## loop to next i_str
        logger.critical('exiting Pipeline.run')

    def _process_task(self, start_count, i_str, start_chunk_time):
        '''
        returns number of stream items processed
        '''
        ## the extractor returns generators of StreamItems
        try:
            i_chunk = self._extractor(i_str)
        except FailedExtraction, exc:
            ## means that task is invalid, extractor did its best
            ## and gave up, so record it in task_queue as failed
            logger.critical('committing failure_log on %s: %r' % (
                    i_str, str(exc)))
            self._task_queue.commit(0, [], failure_log=str(exc))
            return 0

        ## t_path points to the currently in-progress temp chunk
        t_path = None

        ## loop over all docs in the chunk processing and cutting
        ## smaller chunks if needed

        len_clean_visible = 0
        sources = set()
        next_idx = 0

        input_item_limit = self.config.get('input_item_limit')
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
                logger.debug('pipeline yielded to gevent hub')
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
                t_path = os.path.join(self.config['tmp_dir_path'], 'trec-kba-pipeline-tmp-%s' % str(uuid.uuid4()))
                self.t_chunk = streamcorpus.Chunk(path=t_path, mode='wb')

            # TODO: a set of incremental transforms is equivalent to a batch transform.
            # Make the pipeline explicitly configurable as such:
            # batch_transforms: [[incr set 1], big batch op, [incr set 2], ...]
            # OR: for some list of transforms (mixed incremental and batch) pipeline can detect and batchify as needed

            ## incremental transforms populate t_chunk
            ## let the incremental transforms destroy the si by returning None
            si = self._run_incremental_transforms(si, self._incremental_transforms)

            ## insist that every chunk has only one source string
            if si:
                sources.add( si.source )
                assert len(sources) == 1, sources

            if si and si.body and si.body.clean_visible:
                len_clean_visible += len(si.body.clean_visible)
                ## log binned clean_visible lengths, for quick stats estimates
                #logger.debug('len(si.body.clean_visible)=%d' % int(10 * int(math.floor(float(len(si.body.clean_visible)) / 2**10)/10)))
                logger.debug('len(si.body.clean_visible)=%d' % len(si.body.clean_visible))

            if 'output_chunk_max_count' in self.config and \
                    len(self.t_chunk) == self.config['output_chunk_max_count']:
                logger.warn('reached output_chunk_max_count (%d) at: %d' % (len(self.t_chunk), next_idx))
                self.t_chunk.close()
                self._intermediate_output_chunk(start_count, next_idx, sources, i_str, t_path)
                start_count = next_idx

            elif 'output_chunk_max_clean_visible_bytes' in self.config and \
                    len_clean_visible >= self.config['output_chunk_max_clean_visible_bytes']:
                logger.warn('reached output_chunk_max_clean_visible_bytes (%d) at: %d' % (
                        self.config['output_chunk_max_clean_visible_bytes'], len_clean_visible))
                len_clean_visible = 0
                self.t_chunk.close()
                self._intermediate_output_chunk(start_count, next_idx, sources, i_str, t_path)
                start_count = next_idx

            input_item_count += 1
            if (input_item_limit is not None) and (input_item_count > input_item_limit):
                break

        if self.t_chunk:
            self.t_chunk.close()
            o_paths = self._process_output_chunk(start_count, next_idx, sources, i_str, t_path)
        else:
            o_paths = None

        ## put the o_paths into the task_queue, and set
        ## the task to 'completed'
        logger.debug('commit( %d, %r )', next_idx, o_paths)
        self._task_queue.commit( next_idx, o_paths )

        # return how many stream items we processed
        return next_idx

    def _intermediate_output_chunk(self, start_count, next_idx, sources, i_str, t_path):
        o_paths = self._process_output_chunk(start_count, next_idx, sources, i_str, t_path)

        if self._shutting_down:
            return

        ## commit the paths saved so far
        logger.debug('partial_commit( %d, %d, %r )', start_count, next_idx, o_paths)
        self._task_queue.partial_commit( start_count, next_idx, o_paths )

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
          3. run 'loaders' to load-out the data to files or other storage
        return list of paths that loaders wrote to
        '''
        ## gather the paths as the loaders run
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
                self._run_loaders(start_count, next_idx, sources, i_str, t_path, o_paths)
        return o_paths

    def _run_batch_transforms(self, chunk_path):
        for transform in self._batch_transforms:
            try:
                transform.process_path(chunk_path)
            except _exceptions.PipelineOutOfMemory, exc:
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
        if self._pbi_stages:
            t_path2 = os.path.join(self.config['tmp_dir_path'], 'trec-kba-pipeline-tmp-%s' % str(uuid.uuid1()))
            # open destination for _run_incremental_transforms to write to
            self.t_chunk = streamcorpus.Chunk(path=t_path2, mode='wb')

            input_t_chunk = streamcorpus.Chunk(path=t_path, mode='rb')
            for si in input_t_chunk:
                self._run_incremental_transforms(si, self._pbi_stages)

            self.t_chunk.close()

            os.rename(t_path2, tpath)

    def _run_loaders(self, start_count, next_idx, sources, i_str, t_path, o_paths):
        ## loaders put the chunk somewhere, and could delete it
        name_info = dict(
            first = start_count,
            #num and md5 computed in each loaders
            source = sources.pop(),
            )

        for loader in self._loaders:
            try:
                logger.debug('running %r on %r: %r', loader, i_str, name_info)
                o_path = loader(t_path, name_info, i_str)                        
            except OSError, exc:
                if exc.errno == 12:
                    logger.critical('caught OSError 12 in loader, so shutting down')
                    self.shutdown( msg=traceback.format_exc(exc) )
                else:
                    logger.critical('loader (%r, %r) failed', loader, i_str, exc_info=True)
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

            except _exceptions.TransformGivingUp:
                ## do nothing
                logger.info('transform %r giving up on %r' % (transform, si.stream_id))

            except Exception, exc:
                logger.critical('incremental transform failed: %r', transform, exc_info=True)

                if self.config.get('embedded_logs', None):
                    si.body.logs.append( traceback.format_exc(exc) )

                if self.config.get('log_dir_path', None):
                    log_full_file(si, 'fallback-givingup', self.config['log_dir_path'])

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
        self.t_chunk.add(si)

        return si
