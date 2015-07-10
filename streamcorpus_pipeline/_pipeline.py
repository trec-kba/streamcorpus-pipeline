#!/usr/bin/env python
'''Configuration and execution of the actual pipeline.

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2015 Diffeo, Inc.

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

Intermediate files are stored in a subdirectory of :file:`directory`.
The pipeline execution will make an effort to clean this up.  If
omitted, defaults to :file:`/tmp`.

.. code-block:: yaml

    third_dir_path: directory

External packages such as NLP taggers are stored in subdirectories
of :file:`directory`; these are typically individually configured
with `path_in_third` options relative to this directory.

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

After execution finishes, delete the per-execution subdirectory of
``tmp_dir_path``.

.. code-block:: yaml

    assert_single_source: false

Normally a set of stream items has a consistent
:attr:`streamcorpus.StreamItem.source` value, and the pipeline
framework will stop if it sees different values here.  Set this to
``false`` to disable this check.  (Default: do assert a single source
value)

.. code-block:: yaml

    output_chunk_max_count: 500

After this many items have been written, close and re-open the output.
(Default: 500 items)

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
import logging
import os
import threading
import time
import uuid

try:
    import gevent
except ImportError:
    gevent = None

import streamcorpus
from streamcorpus_pipeline._exceptions import TransformGivingUp, \
    InvalidStreamItem
from streamcorpus_pipeline.util import rmtree

logger = logging.getLogger(__name__)


class PipelineFactory(object):
    '''Factory to create :class:`Pipeline` objects from configuration.

    Call this to get a :class:`Pipeline` object.  Typical programmatic
    use:

    .. code-block:: python

       parser = argparse.ArgumentParser()
       args = yakonfig.parse_args([yakonfig, streamcorpus_pipeline])
       factory = PipelineFactory(StageRegistry())
       pipeline = factory(yakonfig.get_global_config('streamcorpus_pipeline'))

    This factory class will instantiate all of the stages named in the
    `streamcorpus_pipeline` configuration.  These stages will be created
    with their corresponding configuration, except that they have two
    keys added, ``tmp_dir_path`` and ``third_dir_path``, from the
    top-level configuration.

    .. automethod:: __init__
    .. automethod:: __call__

    .. attribute:: registry

       The :class:`streamcorpus_pipeline.stages.StageRegistry` used
       to find pipeline stages.

    .. attribute:: tmp_dir_suffix

       A string value that is appended to ``tmp_dir_path`` when
       creating pipeline stages.  If :const:`None`, use the top-level
       ``tmp_dir_path`` configuration directly.

    .. attribute:: lock

       A :class:`threading.Lock` to protect against concurrent
       modification of `tmp_dir_suffix`.

    '''

    def __init__(self, registry):
        '''Create a pipeline factory.

        :param dict config: top-level "streamcorpus_pipeline" configuration
        :param registry: registry of stages
        :type registry: :class:`~streamcorpus_pipeline.stages.StageRegistry`

        '''
        super(PipelineFactory, self).__init__()
        self.registry = registry
        self.lock = threading.Lock()
        self.tmp_dir_suffix = None

    def create(self, stage, scp_config, config=None):
        '''Create a pipeline stage.

        Instantiates `stage` with `config`.  This essentially
        translates to ``stage(config)``, except that two keys from
        `scp_config` are injected into the configuration:
        ``tmp_dir_path`` is an execution-specific directory from
        combining the top-level ``tmp_dir_path`` configuration with
        :attr:`tmp_dir_suffix`; and ``third_dir_path`` is the same
        path from the top-level configuration.  `stage` may be either
        a callable returning the stage (e.g. its class), or its name
        in the configuration.

        `scp_config` is the configuration for the pipeline as a
        whole, and is required.  `config` is the configuration for
        the stage; if it is :const:`None` then it is extracted
        from `scp_config`.

        If you already have a fully formed configuration block
        and want to create a stage, you can call

        .. code-block:: python

            factory.registry[stage](stage_config)

        In most cases if you have a stage class object and want to
        instantiate it with its defaults you can call

        .. code-block:: python

            stage = stage_cls(stage_cls.default_config)

        .. note:: This mirrors
                  :meth:`yakonfig.factory.AutoFactory.create`, with
                  some thought that this factory class might migrate
                  to using that as a base in the future.

        :param stage: pipeline stage class, or its name in the registry
        :param dict scp_config: configuration block for the pipeline
        :param dict config: configuration block for the stage, or
          :const:`None` to get it from `scp_config`

        '''
        # Figure out what we have for a stage and its name
        if isinstance(stage, basestring):
            stage_name = stage
            stage_obj = self.registry[stage_name]
        else:
            stage_name = getattr(stage, 'config_name', stage.__name__)
            stage_obj = stage

        # Find the configuration; get a copy we can mutate
        if config is None:
            config = scp_config.get(stage_name, None)
        if config is None:
            config = getattr(stage_obj, 'default_config', {})
        config = dict(config)

        # Fill in more values
        if self.tmp_dir_suffix is None:
            config['tmp_dir_path'] = scp_config['tmp_dir_path']
        else:
            config['tmp_dir_path'] = os.path.join(scp_config['tmp_dir_path'],
                                                  self.tmp_dir_suffix)
        config['third_dir_path'] = scp_config['third_dir_path']

        return stage_obj(config)

    def _init_stage(self, config, name):

        '''Create a single indirect stage.

        `name` should be the name of a config item that holds the
        name of a stage, for instance, ``reader``.  This looks up
        the name of that stage, then creates and returns the
        stage named.  For instance, if the config says

        .. code-block:: yaml

            reader: from_local_chunks

        then calling ``self._init_stage(scp_config, 'reader')`` will
        return a new instance of the
        :class:`~streamcorpus_pipeline._local_storage.from_local_chunks`
        stage.

        :param dict config: `streamcorpus_pipeline` configuration block
        :param str name: name of stage name entry
        :return: new instance of the stage

        '''
        return self.create(config[name], config)

    def _init_stages(self, config, name):
        '''Create a list of indirect stages.

        `name` should be the name of a config item that holds a list
        of names of stages, for instance, ``writers``.  This looks up
        the names of those stages, then creates and returns the
        corresponding list of stage objects.  For instance, if the
        config says

        .. code-block:: yaml

            incremental_transforms: [clean_html, clean_visible]

        then calling ``self._init_stages(scp_config,
        'incremental_transforms')`` will return a list of the two
        named stage instances.

        :param dict config: `streamcorpus_pipeline` configuration block
        :param str name: name of the stage name list entry
        :return: list of new stage instances

        '''
        if name not in config:
            return []
        return [self.create(stage, config) for stage in config[name]]

    def _init_all_stages(self, config):
        '''Create stages that are used for the pipeline.

        :param dict config: `streamcorpus_pipeline` configuration
        :return: tuple of (reader, incremental transforms, batch
          transforms, post-batch incremental transforms, writers,
          temporary directory)

        '''
        reader = self._init_stage(config, 'reader')
        incremental_transforms = self._init_stages(
            config, 'incremental_transforms')
        batch_transforms = self._init_stages(config, 'batch_transforms')
        post_batch_incremental_transforms = self._init_stages(
            config, 'post_batch_incremental_transforms')
        writers = self._init_stages(config, 'writers')
        tmp_dir_path = os.path.join(config['tmp_dir_path'],
                                    self.tmp_dir_suffix)
        return (reader, incremental_transforms, batch_transforms,
                post_batch_incremental_transforms, writers, tmp_dir_path)

    def __call__(self, config):
        '''Create a :class:`Pipeline`.

        Pass in the configuration under the ``streamcorpus_pipeline``
        block, not the top-level configuration that contains it.

        If :attr:`tmp_dir_suffix` is :const:`None`, then locks the
        factory and creates stages with a temporary (UUID) value.  If
        the configuration has `cleanup_tmp_files` set to :const:`True`
        (the default) then executing the resulting pipeline will clean
        up the directory afterwards.

        :param dict config: `streamcorpus_pipeline` configuration
        :return: new pipeline instance

        '''
        tmp_dir_suffix = self.tmp_dir_suffix
        if tmp_dir_suffix is None:
            with self.lock:
                self.tmp_dir_suffix = str(uuid.uuid4())
                try:
                    (reader, incremental_transforms, batch_transforms,
                     pbi_transforms, writers,
                     tmp_dir_path) = self._init_all_stages(config)
                finally:
                    self.tmp_dir_suffix = None
        else:
            (reader, incremental_transforms, batch_transforms,
             pbi_transforms, writers,
             tmp_dir_path) = self._init_all_stages(config)

        return Pipeline(
            rate_log_interval=config['rate_log_interval'],
            input_item_limit=config.get('input_item_limit'),
            cleanup_tmp_files=config['cleanup_tmp_files'],
            tmp_dir_path=tmp_dir_path,
            assert_single_source=config['assert_single_source'],
            output_chunk_max_count=config.get('output_chunk_max_count'),
            output_max_clean_visible_bytes=config.get(
                'output_max_clean_visible_bytes'),
            reader=reader,
            incremental_transforms=incremental_transforms,
            batch_transforms=batch_transforms,
            post_batch_incremental_transforms=pbi_transforms,
            writers=writers,
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
            i_str=None,
            data=None,
            )
        self.work_unit = None

    def _process_task(self, work_unit):
        '''Process a :class:`coordinate.WorkUnit`.

        The work unit's key is taken as the input file name.  The
        data should have ``start_count`` and ``start_chunk_time``
        values, which are passed on to :meth:`run`.

        :param work_unit: work unit to process
        :paramtype work_unit: :class:`coordinate.WorkUnit`
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
                    sources.add(si.source)
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

                if ((self.output_chunk_max_count is not None and
                     len(self.t_chunk) == self.output_chunk_max_count)):
                    logger.info('reached output_chunk_max_count (%d) at: %d',
                                len(self.t_chunk), next_idx)
                    self._process_output_chunk(
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
                    self._process_output_chunk(
                        start_count, next_idx, sources, i_str, t_path)
                    start_count = next_idx

                input_item_count += 1
                if (((self.input_item_limit is not None) and
                     (input_item_count > self.input_item_limit))):
                    break

            if self.t_chunk is not None:
                self._process_output_chunk(
                    start_count, next_idx, sources, i_str, t_path)

            ## return how many stream items we processed
            return next_idx

        finally:
            if self.t_chunk is not None:
                self.t_chunk.close()
            for transform in self.batch_transforms:
                transform.shutdown()
            if self.cleanup_tmp_files:
                rmtree(self.tmp_dir_path)

    def _process_output_chunk(self, start_count, next_idx, sources, i_str,
                              t_path):
        '''
        for the current output chunk (which should be open):
          1. run batch transforms
          2. run post-batch incremental transforms
          3. run 'writers' to load-out the data to files or other storage
        return list of paths that writers wrote to
        '''
        if not self.t_chunk:
            # nothing to do
            return []
        self.t_chunk.close()

        # gather the paths as the writers run
        o_paths = None
        if len(self.t_chunk) > 0:
            # only batch transform and load if the chunk
            # isn't empty, which can happen when filtering
            # with stages like "find"

            # batch transforms act on the whole chunk in-place
            logger.info('running batch transforms on %d StreamItems',
                        len(self.t_chunk))
            self._run_batch_transforms(t_path)

            self._maybe_run_post_batch_incremental_transforms(t_path)

            # only proceed if above transforms left us with something
            if (self.t_chunk) and (len(self.t_chunk) >= 0):
                o_paths = self._run_writers(start_count, next_idx, sources,
                                            i_str, t_path)

        # we're now officially done with the chunk
        self.t_chunk = None

        # If we wrote some paths, update the data dictionary of outputs
        if self.work_unit and o_paths:
            old_o_paths = self.work_unit.data.get('output', [])
            o_paths = old_o_paths + o_paths
            self.work_unit.data['start_count'] = next_idx
            self.work_unit.data['output'] = o_paths
            self.work_unit.update()

    def _run_batch_transforms(self, chunk_path):
        '''Run all of the batch transforms over some intermediate chunk.'''
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

    def _run_writers(self, start_count, next_idx, sources, i_str, t_path):
        '''Run all of the writers over some intermediate chunk.

        :param int start_count: index of the first item
        :param int next_idx: index of the next item (after the last
          item in this chunk)
        :param list sources: source strings included in this chunk
          (usually only one source)
        :param str i_str: name of input file or other input
        :param str t_path: location of intermediate chunk on disk
        :return: list of output file paths or other outputs

        '''
        # writers put the chunk somewhere, and could delete it
        name_info = dict(
            first=start_count,
            # num and md5 computed in each writers
            source=sources.pop(),
            )

        all_o_paths = []
        for writer in self.writers:
            logger.debug('running %r on %r: %r', writer, i_str, name_info)
            o_paths = writer(t_path, name_info, i_str)
            logger.debug('loaded (%d, %d) of %r into %r',
                         start_count, next_idx - 1, i_str, o_paths)
            all_o_paths += o_paths
        return all_o_paths

    def _run_incremental_transforms(self, si, transforms):
        '''
        Run transforms on stream item.
        Item may be discarded by some transform.
        Writes successful items out to current self.t_chunk
        Returns transformed item or None.
        '''
        ## operate each transform on this one StreamItem
        for transform in transforms:
            try:
                stream_id = si.stream_id
                si_new = transform(si, context=self.context)

                if si_new is None:
                    logger.warn('transform %r deleted %s abs_url=%r',
                                 transform, stream_id, si and si.abs_url)
                    return None
                si = si_new

            except TransformGivingUp:
                ## do nothing
                logger.info('transform %r giving up on %r',
                            transform, si.stream_id)

            except Exception, exc:
                logger.critical(
                    'transform %r failed on %r from i_str=%r abs_url=%r',
                    transform, si and si.stream_id, self.context.get('i_str'),
                    si and si.abs_url, exc_info=True)

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
