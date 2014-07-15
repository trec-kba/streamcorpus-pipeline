#!/usr/bin/env python
'''Configuration and execution of the actual pipeline.

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2014 Diffeo, Inc.

The :class:`Pipeline` itself consists of a series of
:mod:`~streamcorpus_pipeline.stages`.  These are broken into several
categories:

* Exactly one *reader* runs first, producing a sequence of
  :class:`streamcorpus.StreamItem` objects.

* The stream items are fed through *incremental transforms*, which take
  one stream item in and produce one stream item out.

* All of the stream items are written to a file, and *batch transforms*
  can operate on the entire collection of stream items at once.

* *Post-batch incremental transforms* again operate on individual
  stream items.

* Some number of *writers* send the final output somewhere.

Configuration
=============

The :command:`streamcorpus_pipeline` tool expects configuration in
a YAML file.  The configuration resulting from this can be passed
through :class:`PipelineFactory` to create :class:`Pipeline` objects.
A typical coniguration looks like:

.. code-block:: yaml

    streamcorpus_pipeline:
      # This setting is REQUIRED, it is typically set to a unique
      # path per execution that can be easily cleaned up.
      tmp_dir_path: /tmp/streamcorpus_pipeline-987

      # Lists of stages
      reader: from_local_chunks
      incremental_transforms: [clean_html, language]
      batch_transforms: []
      post_batch_incremental_transforms: []
      # to_local_chunks must be the last writer if it is used
      writers: [to_local_chunks]

      # Configuration for specific stages
      clean_html:
        include_language_codes: [en]

The ``streamcorpus_pipeline`` block can additionally be configured
with:

.. code-block:: yaml

    root_path: /home/user/diffeo

Any configuration variable whose name ends in "path" whose value is
not an absolute path is considered relative to this directory.  If
omitted, use the current directory.

.. code-block:: yaml

    tmp_dir_path: directory

Intermediate files are stored in :file:`directory`.  The pipeline
execution will make an effort to clean this up.

.. note:: The user is *required* to set ``tmp_dir_path``, there is no
          default.

.. code-block:: yaml

    rate_log_interval: 500

When this many items have been processed, log an INFO-level log
message giving the current progress.

.. code-block:: yaml

    input_item_limit: 500

Stop processing after reading this many stream items.  (Default:
process the entire stream)

.. code-block:: yaml

    cleanup_tmp_files: true

After execution finishes, delete ``tmp_dir_path``.

.. code-block:: yaml

    assert_single_source: false

Normally a set of stream items has a consistent
:attr:`streamcorpus.StreamItem.source` value, and the pipeline
framework will stop if it sees different values here.  Set this to
``false`` to disable this check.  (Default: do assert a single source
value)

.. code-block:: yaml

    output_max_chunk_count: 500

After this many items have been written, close and re-open the output.
(Default: write entire output in one batch)

.. code-block:: yaml

    output_max_clean_visible_bytes: 1000000

After this much :attr:`streamcorpus.StreamItem.clean_visible` content
has been written, close and re-open the output.  (Default: write
entire output in one batch)

.. code-block:: yaml

    external_stages_path: stages.py

The file :file:`stages.py` is a Python module that declares a
top-level dictionary named `Stages`, a map from stage name to
implementing class.  Stages defined in this file can be used in any of
the appropriate stage lists.

.. code-block:: yaml

    external_stages_modules: [ example.stages ]

The Python module :mod:`example.stages` declares a top-level
dictionary named `Stages`, a map from stage name to implementing
class.  The named modules must be on :data:`sys.path` so that the
Python interpreter can find it.  Stages defined in these modules can
be used in any of the appropriate stage lists.

API
===

The standard execution path is to pass the
:mod:`streamcorpus_pipeline` module to
:func:`yakonfig.parse_args`, then use :class:`PipelineFactory` to
create :class:`Pipeline` objects from the resulting configuration.

.. todo:: Make the top-level configuration point
          :class:`PipelineFactory`, not the
          :mod:`streamcorpus_pipeline` module

.. autoclass:: PipelineFactory
   :members:

.. autoclass:: Pipeline
   :members:

'''

from __future__ import absolute_import
import itertools
import logging
import math
import os
import re
import shutil
import sys
import time
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
from streamcorpus_pipeline.util import rmtree

logger = logging.getLogger(__name__)

class PipelineFactory(object):
    '''Factory to create `Pipeline` objects from configuration.

    Call this to get a `Pipeline` object.  Typical programmatic use:

    >>> parser = argparse.ArgumentParser()
    >>> args = yakonfig.parse_args([yakonfig, streamcorpus_pipeline])
    >>> factory = PipelineFactory(StageRegistry())
    >>> pipeline = factory(yakonfig.get_global_config('streamcorpus_pipeline'))

    .. automethod:: __init__
    .. automethod:: __call__
    '''

    def __init__(self, registry, *args, **kwargs):
        '''Create a pipeline factory.

        :param dict config: top-level "streamcorpus_pipeline" configuration
        :param registry: registry of stages
        :type registry: :class:`~streamcorpus_pipeline.stages.StageRegistry`

        '''
        super(PipelineFactory, self).__init__(*args, **kwargs)
        self.registry = registry

    def _init_stage(self, config, name):
        return self.registry.init_stage(config[name], config)

    def _init_stages(self, config, name):
        if name not in config:
            return []
        return [self.registry.init_stage(stage, config)
                for stage in config[name]]

    def __call__(self, config):
        '''Create a :class:`Pipeline`.

        Pass in the configuration under the ``streamcorpus_pipeline``
        block, not the top-level configuration that contains it.
        '''
        return Pipeline(
            rate_log_interval=config['rate_log_interval'],
            input_item_limit=config.get('input_item_limit'),
            cleanup_tmp_files=config['cleanup_tmp_files'],
            tmp_dir_path=config['tmp_dir_path'],
            assert_single_source=config['assert_single_source'],
            output_chunk_max_count=config.get('output_chunk_max_count'),
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

    .. automethod:: __init__
    .. automethod:: run
    .. automethod:: _process_task

    '''
    def __init__(self, rate_log_interval, input_item_limit,
                 cleanup_tmp_files, tmp_dir_path, assert_single_source,
                 output_chunk_max_count, output_max_clean_visible_bytes,
                 reader, incremental_transforms, batch_transforms,
                 post_batch_incremental_transforms, writers):
        '''Create a new pipeline object.

        .. todo:: make this callable with just the lists of stages
                  and give sane (language-level) defaults for the rest

        :param int rate_log_interval: print progress every time this
          many input items have been processed
        :param int input_item_limit: stop after this many items
        :param bool cleanup_tmp_files: delete `tmp_dir_path` after
          execution if true
        :param str tmp_dir_path: path for intermediate files
        :param bool assert_single_source: require all items to have
          the same source value if true
        :param int output_chunk_max_count: restart output after
          writing this many items
        :param int output_max_clean_visible_bytes: restart output after
          writing this much content
        :param callable reader: reader stage object
        :param incremental_transforms: single-item transformation stages
        :paramtype incremental_transforms: list of callable
        :param batch_transforms: chunk-file transformation stages
        :paramtype batch_transforms: list of callable
        :param post_batch_incremental_transforms: single-item transformation
          stages
        :paramtype post_batch_incremental_transforms: list of callable
        :param writers: output stages
        :paramtype writers: list of callable

        '''
        self.rate_log_interval = rate_log_interval
        self.input_item_limit = input_item_limit
        self.cleanup_tmp_files = cleanup_tmp_files
        self.tmp_dir_path = tmp_dir_path
        self.assert_single_source = assert_single_source
        self.output_chunk_max_count = output_chunk_max_count
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

    def _process_task(self, work_unit):
        '''Process a :class:`rejester.WorkUnit`.

        The work unit's key is taken as the input file name.  The
        data should have ``start_count`` and ``start_chunk_time``
        values, which are passed on to :meth:`run`.

        :param work_unit: work unit to process
        :paramtype work_unit: :class:`rejester.WorkUnit`
        :return: number of stream items processed

        '''
        self.work_unit = work_unit
        i_str = work_unit.key
        start_count = work_unit.data['start_count']
        start_chunk_time = work_unit.data['start_chunk_time']
        self.run(i_str, start_count, start_chunk_time)

    def run(self, i_str, start_count=0, start_chunk_time=None):
        '''Run the pipeline.

        This runs all of the steps described in the pipeline constructor,
        reading from some input and writing to some output.

        :param str i_str: name of the input file, or other reader-specific
          description of where to get input
        :param int start_count: index of the first stream item
        :param int start_chunk_time: timestamp for the first stream item

        '''
        try:
            if not os.path.exists(self.tmp_dir_path):
                os.makedirs(self.tmp_dir_path)

            if start_chunk_time is None:
                start_chunk_time = time.time()

            ## the reader returns generators of StreamItems
            i_chunk = self.reader(i_str)

            ## t_path points to the currently in-progress temp chunk
            t_path = None

            ## loop over all docs in the chunk processing and cutting
            ## smaller chunks if needed

            len_clean_visible = 0
            sources = set()
            next_idx = 0

            ## how many have we input and actually done processing on?
            input_item_count = 0

            for si in i_chunk:
                # TODO: break out a _process_stream_item function?
                next_idx += 1

                ## yield to the gevent hub to allow other things to run
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
                        logger.info('%d in %.1f --> %.1f per sec on '
                                    '(pre-partial_commit) %s',
                                    next_idx - start_count, elapsed, rate,
                                    i_str)

                if not self.t_chunk:
                    ## make a temporary chunk at a temporary path
                    # (Lazy allocation after we've read an item that might get processed out to the new chunk file)
                    # TODO: make this EVEN LAZIER by not opening the t_chunk until inside _run_incremental_transforms whe the first output si is ready
                    t_path = os.path.join(self.tmp_dir_path,
                                          't_chunk-%s' % uuid.uuid4().hex)
                    self.t_chunk = streamcorpus.Chunk(path=t_path, mode='wb')
                    assert self.t_chunk.message == streamcorpus.StreamItem_v0_3_0, self.t_chunk.message

                # TODO: a set of incremental transforms is equivalent
                # to a batch transform.  Make the pipeline explicitly
                # configurable as such:
                #
                # batch_transforms: [[incr set 1], batch op, [incr set 2], ...]
                #
                # OR: for some list of transforms (mixed incremental
                # and batch) pipeline can detect and batchify as needed

                ## incremental transforms populate t_chunk
                ## let the incremental transforms destroy the si by
                ## returning None
                si = self._run_incremental_transforms(
                    si, self.incremental_transforms)

                ## insist that every chunk has only one source string
                if si:
                    sources.add( si.source )
                    if self.assert_single_source and len(sources) != 1:
                        raise InvalidStreamItem(
                            'stream item %r had source %r, not %r '
                            '(set assert_single_source: false to suppress)' %
                            (si.stream_id, si.source, sources))

                if si and si.body and si.body.clean_visible:
                    len_clean_visible += len(si.body.clean_visible)
                    ## log binned clean_visible lengths, for quick stats estimates
                    #logger.debug('len(si.body.clean_visible)=%d' % int(10 * int(math.floor(float(len(si.body.clean_visible)) / 2**10)/10)))
                    #logger.debug('len(si.body.clean_visible)=%d' % len(si.body.clean_visible))


                if (self.output_chunk_max_count is not None and
                    len(self.t_chunk) == self.output_chunk_max_count):
                    logger.info('reached output_chunk_max_count (%d) at: %d',
                                len(self.t_chunk), next_idx)
                    self.t_chunk.close()
                    self._intermediate_output_chunk(
                        start_count, next_idx, sources, i_str, t_path)
                    start_count = next_idx

                elif (self.output_max_clean_visible_bytes is not None and
                      len_clean_visible >=
                      self.output_chunk_max_clean_visible_bytes):
                    logger.info(
                        'reached output_chunk_max_clean_visible_bytes '
                        '(%d) at: %d',
                        self.output_chunk_max_clean_visible_bytes,
                        len_clean_visible)
                    len_clean_visible = 0
                    self.t_chunk.close()
                    self._intermediate_output_chunk(
                        start_count, next_idx, sources, i_str, t_path)
                    start_count = next_idx

                input_item_count += 1
                if ((self.input_item_limit is not None) and
                    (input_item_count > self.input_item_limit)):
                    break

            ## bool(t_chunk) is False if t_chunk has no data, but we still
            ## want to make sure it gets closed.
            if self.t_chunk is not None:
                self.t_chunk.close()
                o_paths = self._process_output_chunk(
                    start_count, next_idx, sources, i_str, t_path)
                self.t_chunk = None
            else:
                o_paths = None

            ## set start_count and o_paths in work_unit and updated
            data = dict(start_count=next_idx, o_paths=o_paths)
            logger.debug('WorkUnit.update() data=%r', data)
            if self.work_unit is not None:
                self.work_unit.data.update(data)
                self.work_unit.update()

            ## return how many stream items we processed
            return next_idx

        finally:
            if self.t_chunk is not None:
                self.t_chunk.close()
            for transform in self.batch_transforms:
                transform.shutdown()
            if self.cleanup_tmp_files:
                rmtree(self.tmp_dir_path)
            

    def _intermediate_output_chunk(self, start_count, next_idx, sources, i_str, t_path):
        '''save a chunk that is smaller than the input chunk
        '''
        o_paths = self._process_output_chunk(start_count, next_idx, sources, i_str, t_path)

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
            logger.info('running batch transforms on %d StreamItems', len(self.t_chunk))
            self._run_batch_transforms(t_path)

            self._maybe_run_post_batch_incremental_transforms(t_path)

            # only proceed if above transforms left us with something
            if (self.t_chunk) and (len(self.t_chunk) >= 0):
                self._run_writers(start_count, next_idx, sources, i_str, t_path, o_paths)
        if self.t_chunk is not None:
            self.t_chunk.close()                
        return o_paths

    def _run_batch_transforms(self, chunk_path):
        for transform in self.batch_transforms:
            transform.process_path(chunk_path)

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
            logger.debug('running %r on %r: %r', writer, i_str, name_info)
            o_path = writer(t_path, name_info, i_str) 
            logger.debug('loaded (%d, %d) of %r into %r',
                         start_count, next_idx - 1, i_str, o_path)
            if o_path:
                o_paths.append( o_path )

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
                stream_id = si.stream_id
                si = transform(si, context=self.context)

                if si is None:
                    logger.warn('transform %r deleted %s',
                                 transform, stream_id)
                    return None

            except TransformGivingUp:
                ## do nothing
                logger.info('transform %r giving up on %r',
                            transform, si.stream_id)

            except Exception, exc:
                logger.critical('transform %r failed on %r from i_str=%r', 
                                transform, si and si.stream_id, self.context.get('i_str'),
                                exc_info=True)

        assert si is not None

        ## expect to always have a stream_time
        if not si.stream_time:
            raise InvalidStreamItem('empty stream_time: %s' % si)

        if si.stream_id is None:
            raise InvalidStreamItem('empty stream_id: %r' % si)

        ## put the StreamItem into the output
        if type(si) != streamcorpus.StreamItem_v0_3_0:
            raise InvalidStreamItem('incorrect stream item object %r' %
                                    type(si))
        self.t_chunk.add(si)
        return si
