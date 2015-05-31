#!/usr/bin/env python
'''Registry of stages for streamcorpus_pipeline.

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2015 Diffeo, Inc.

Stages are implemented as callable Python objects.  In almost all
cases the name of the stage in the configuration file is the same as
its class name.

Readers
=======

.. autoclass:: streamcorpus_pipeline._convert_kba_json.convert_kba_json
.. autoclass:: streamcorpus_pipeline._local_storage.from_local_chunks
.. autoclass:: streamcorpus_pipeline._local_storage.from_local_files
.. autoclass:: streamcorpus_pipeline._kvlayer.from_kvlayer
.. autoclass:: streamcorpus_pipeline._s3_storage.from_s3_chunks
.. autoclass:: streamcorpus_pipeline._john_smith.john_smith
.. autoclass:: streamcorpus_pipeline._yaml_files_list.yaml_files_list
.. autoclass:: streamcorpus_pipeline._spinn3r_feed_storage.from_spinn3r_feed
.. autoclass:: streamcorpus_pipeline._serifxml.from_serifxml

Incremental transforms
======================

.. autoclass:: streamcorpus_pipeline._clean_html.clean_html
.. autoclass:: streamcorpus_pipeline.force_clean_html.force_clean_html
.. autoclass:: streamcorpus_pipeline._clean_visible.clean_visible
.. autoclass:: streamcorpus_pipeline.offsets.xpath_offsets
.. autoclass:: streamcorpus_pipeline._pdf_to_text.pdf_to_text
.. autoclass:: streamcorpus_pipeline._docx_to_text.docx_to_text
.. autoclass:: streamcorpus_pipeline._title.title
.. autoclass:: streamcorpus_pipeline._filters.debug_filter
.. autoclass:: streamcorpus_pipeline._filters.filter_domains
.. autoclass:: streamcorpus_pipeline._filters.filter_domains_substrings
.. autoclass:: streamcorpus_pipeline._fix_text.fix_text
.. autoclass:: streamcorpus_pipeline._dedup.dedup
.. autoclass:: streamcorpus_pipeline._dump_label_stats.dump_label_stats
.. autoclass:: streamcorpus_pipeline._filters.id_filter
.. autofunction:: streamcorpus_pipeline._guess_media_type.file_type_stats
.. autoclass:: streamcorpus_pipeline._filters.filter_languages
.. autoclass:: streamcorpus_pipeline._filters.filter_tagger_ids
.. autoclass:: streamcorpus_pipeline._find.find
.. autoclass:: streamcorpus_pipeline._find.find_doc_ids
.. autoclass:: streamcorpus_pipeline._guess_media_type.guess_media_type
.. autoclass:: streamcorpus_pipeline._handle_unconvertible_spinn3r.handle_unconvertible_spinn3r
.. autoclass:: streamcorpus_pipeline._hyperlink_labels.hyperlink_labels
.. autoclass:: streamcorpus_pipeline._upgrade_streamcorpus.keep_annotated
.. autoclass:: streamcorpus_pipeline._language.language
.. autoclass:: streamcorpus_pipeline._nilsimsa.nilsimsa
.. autoclass:: streamcorpus_pipeline._filters.remove_raw
.. autoclass:: streamcorpus_pipeline._filters.replace_raw
.. autoclass:: streamcorpus_pipeline._filters.dump_stream_id_abs_url
.. autoclass:: streamcorpus_pipeline._upgrade_streamcorpus.upgrade_streamcorpus
.. autoclass:: streamcorpus_pipeline._upgrade_streamcorpus_v0_3_0.upgrade_streamcorpus_v0_3_0
.. autoclass:: streamcorpus_pipeline._tokenizer.nltk_tokenizer

Batch transforms
================

.. autoclass:: streamcorpus_pipeline._taggers.byte_offset_align_labels
.. autoclass:: streamcorpus_pipeline._taggers.line_offset_align_labels
.. autoclass:: streamcorpus_pipeline._taggers.name_align_labels
.. autoclass:: streamcorpus_pipeline._lingpipe.lingpipe
.. autoclass:: streamcorpus_pipeline._serif.serif

Writers
=======

.. autoclass:: streamcorpus_pipeline._local_storage.to_local_chunks
.. autoclass:: streamcorpus_pipeline._local_storage.to_local_tarballs
.. autoclass:: streamcorpus_pipeline._kvlayer.to_kvlayer
.. autoclass:: streamcorpus_pipeline._s3_storage.to_s3_chunks
.. autoclass:: streamcorpus_pipeline._s3_storage.to_s3_tarballs

API
===

All stage objects are constructed with a single parameter, the
dictionary of configuration specific to the stage.  The stage objects
can be passed directly to the ``streamcorpus_pipeline.Pipeline``
constructor.

.. currentmodule:: streamcorpus_pipeline.stages

.. autoclass:: StageRegistry
   :members:
   :show-inheritance:

.. autoclass:: PipelineStages
   :members:
   :show-inheritance:

.. autoclass:: Configured
   :members:
   :show-inheritance:

.. autoclass:: BatchTransform
   :members:
   :show-inheritance:

.. autoclass:: IncrementalTransform
   :members:
   :show-inheritance:

'''
from __future__ import absolute_import
from abc import ABCMeta, abstractmethod
from collections import Callable, MutableMapping
import imp
import logging
import time

import pkg_resources

logger = logging.getLogger(__name__)


class StageRegistry(MutableMapping):
    '''List of known stages for a pipeline.

    This is a dictionary, and so ``registry[stagename]`` will retrieve
    the class object or constructor function for the stage.  Each
    stage in the dictionary is a callable of a single parameter, the
    configuration dictionary, which returns a callable appropriate
    for its stage type.  Typical patterns are actual classes and
    wrapper functions::

        class a_stage(streamcorpus_pipeline.stages.Configurable):
            config_name = 'a_stage'
            def __init__(self, config, *args, **kwargs):
                super(AStage, self).__init__(config, *args, **kwargs)
                self.param = self.config.get('param')
            def __call__(self, si, context):
                logger.debug('param is %s', self.param)
                return si

        def b_stage(config):
            param = config.get('param')
            def do_stage(si, context):
               logger.debug('param is %s', param)
               return si
            return do_stage

        registry = StageRegistry()
        registry['a_stage'] = a_stage
        registry['b_stage'] = b_stage

    '''
    def __init__(self, *args, **kwargs):
        super(StageRegistry, self).__init__(*args, **kwargs)
        self.stages = {}

    # MutableMapping special methods

    def __getitem__(self, k):
        if k not in self.stages:
            raise KeyError('%r not in %r' % (k, self.stages.keys()))
        return self.stages[k]

    def __setitem__(self, k, v):
        self.stages[k] = v

    def __delitem__(self, k):
        del self.stages[k]

    def __iter__(self):
        return self.stages.iterkeys()

    def __len__(self):
        return len(self.stages)

    # StageRegistry-specific methods
    def tryload_stage(self, moduleName, functionName, name=None):
        '''Try to load a stage into self, ignoring errors.

        If loading a module fails because of some subordinate load
        failure, just give a warning and move on.  On success the
        stage is added to the stage dictionary.

        :param str moduleName: name of the Python module
        :param str functionName: name of the stage constructor
        :param str name: name of the stage, defaults to `functionName`

        '''
        if name is None:
            name = functionName
        try:
            mod = __import__(moduleName, globals(), locals(), [functionName])
        except ImportError, exc:
            logger.warn('cannot load stage {0}: cannot load module {1}'
                        .format(name, moduleName), exc_info=exc)
            return

        if not hasattr(mod, functionName):
            logger.warn('cannot load stage {0}: module {1} missing {2}'
                        .format(name, moduleName, functionName))
            return

        self[name] = getattr(mod, functionName)

    def load_external_stages(self, path):
        '''Add external stages from the Python module in `path`.

        `path` must be a path to a Python module source that contains
        a `Stages` dictionary, which is a map from stage name to callable.

        :param str path: path to the module file
        '''
        mod = imp.load_source('', path)
        self.update(mod.Stages)

    def load_module_stages(self, mod):
        '''Add external stages from the Python module `mod`.

        If `mod` is a string, then it will be interpreted as the name
        of a module; otherwise it is an actual module object.  The
        module should exist somewhere in :data:`sys.path`.  The module
        must contain a `Stages` dictionary, which is a map from stage
        name to callable.

        :param mod: name of the module or the module itself
        :raise exceptions.ImportError: if `mod` cannot be loaded or does
          not contain ``Stages``

        '''
        if isinstance(mod, basestring):
            mod = __import__(mod, globals=globals(), locals=locals(),
                             fromlist=['Stages'], level=0)
        if not hasattr(mod, 'Stages'):
            raise ImportError(mod)
        self.update(mod.Stages)

    def init_stage(self, name, config):

        '''Construct and configure a stage from known stages.

        `name` must be the name of one of the stages in this.  `config`
        is the configuration dictionary of the containing object, and its `name`
        member will be passed into the stage constructor.

        :param str name: name of the stage
        :param dict config: parent object configuration
        :return: callable stage
        :raise exceptions.KeyError: if `name` is not a known stage

        '''
        subconfig = config.get(name, {})
        ctor = self[name]
        return ctor(subconfig)


class Configured(object):
    '''Any object containing a configuration.

    The configuration is passed to the constructor.

    .. attribute:: config

        The configuration dictionary for this object.

    '''
    def __init__(self, config, *args, **kwargs):
        super(Configured, self).__init__(*args, **kwargs)
        self.config = config


class BatchTransform(Configured):
    '''Transform that acts on an entire :class:`streamcorpus.Chunk` file.'''
    __metaclass__ = ABCMeta

    @abstractmethod
    def process_path(self, chunk_path):
        '''Process a chunk file at a specific path.

        Implementations should work in place, writing results out to the
        same `chunk_path`.  The pipeline guarantees that this stage
        will have a key ``tmp_dir_path`` in its configuration, and this
        can be used to store an intermediate file which is then renamed
        over `chunk_path`.
        '''
        raise NotImplementedError('BatchTransform.process_path not implemented')

    @abstractmethod
    def shutdown(self):
        '''Do an orderly shutdown of this stage.

        If the stage spawns a child process, kill it, and do any other
        required cleanup.  :meth:`streamcorpus_pipeline.Pipeline._cleanup`
        will call this method on every batch transform stage, regardless
        of whether this is the currently running stage or not.
        '''
        raise NotImplementedError('shutdown is a required method for all BatchTransforms')


class IncrementalTransform(Configured, Callable):
    '''Transform that acts on individual streamcorpus StreamItem objects.

    The pipeline can run any stage with an appropriate :meth:`__call__`
    method.  The :meth:`__call__` or :meth:`process_item` methods are
    called once per stream item, reusing the same transform object.

    '''
    __metaclass__ = ABCMeta
    @abstractmethod
    def process_item(self, stream_item, context):
        '''Process a single :class:`streamcorpus.StreamItem` object.

        `context` is shared across all stages.  It is guaranteed to
        contain ``i_str`` nominally containing the original input file
        name and ``data`` nominally containing the auxiliary data from
        the work item.

        This function may modify `stream_item` in place, construct
        a new item and return it, or return :const:`None` to drop
        the item.

        :param stream_item: stream item to process
        :paramtype stream_item: :class:`streamcorpus.StreamItem`
        :param dict context: additional shared context data
        :return: same or different stream item or :const:`None`

        '''
        raise NotImplementedError('BatchTransform.process_path not implemented')

    def __call__(self, stream_item, context):
        '''Entry point from the pipeline.

        This implementation calls :meth:`process_item`.

        '''
        return self.process_item(stream_item, context)

    def shutdown(self):
        '''Do an orderly shutdown of this stage.

        Incremental transforms are generally simple Python code and
        a complicated shutdown is not required.  The pipeline does not
        call this method.
        '''
        pass


class PipelineStages(StageRegistry):
    '''The list of stages, preloaded with streamcorpus_pipeline stages.'''
    def __init__(self, *args, **kwargs):
        start = time.time()
        super(PipelineStages, self).__init__(*args, **kwargs)

        # data source readers (read data from somewhere into pipeline)
        self.tryload_stage('_convert_kba_json', 'convert_kba_json')
        self.tryload_stage('_local_storage', 'from_local_chunks')
        self.tryload_stage('_local_storage', 'from_local_files')
        self.tryload_stage('_kvlayer', 'from_kvlayer')
        self.tryload_stage('_s3_storage', 'from_s3_chunks')
        self.tryload_stage('_john_smith', 'john_smith')
        self.tryload_stage('_yaml_files_list', 'yaml_files_list')
        self.tryload_stage('_spinn3r_feed_storage', 'from_spinn3r_feed')
        self.tryload_stage('_serifxml', 'from_serifxml')

        # StreamItem stages
        # (alphabetical by stage name)
        self.tryload_stage('_clean_html', 'clean_html')
        self.tryload_stage('force_clean_html', 'force_clean_html')
        self.tryload_stage('_clean_visible', 'clean_visible')
        self.tryload_stage('offsets', 'xpath_offsets')
        self.tryload_stage('_pdf_to_text', 'pdf_to_text')
        self.tryload_stage('_docx_to_text', 'docx_to_text')
        self.tryload_stage('_title', 'title')
        self.tryload_stage('_filters', 'debug_filter')
        self.tryload_stage('_filters', 'filter_domains')
        self.tryload_stage('_filters', 'filter_domains_substrings')
        self.tryload_stage('_fix_text', 'fix_text')
        self.tryload_stage('_dedup', 'dedup')
        self.tryload_stage('_dump_label_stats', 'dump_label_stats')
        self.tryload_stage('_filters', 'id_filter')
        self.tryload_stage('_guess_media_type', 'file_type_stats')
        self.tryload_stage('_filters', 'filter_languages')
        self.tryload_stage('_filters', 'filter_tagger_ids')
        self.tryload_stage('_find', 'find')
        self.tryload_stage('_find', 'find_doc_ids')
        self.tryload_stage('_guess_media_type', 'guess_media_type')
        self.tryload_stage('_handle_unconvertible_spinn3r', 'handle_unconvertible_spinn3r')
        self.tryload_stage('_hyperlink_labels', 'hyperlink_labels')
        self.tryload_stage('_upgrade_streamcorpus', 'keep_annotated')
        self.tryload_stage('_upgrade_streamcorpus', 'keep_annotated', 'keep_annotatoted')
        self.tryload_stage('_language', 'language')
        self.tryload_stage('_nilsimsa', 'nilsimsa')
        self.tryload_stage('_filters', 'remove_raw')
        self.tryload_stage('_filters', 'replace_raw')
        self.tryload_stage('_filters', 'dump_stream_id_abs_url')
        self.tryload_stage('_set_source', 'set_source')
        self.tryload_stage('_upgrade_streamcorpus', 'upgrade_streamcorpus')
        self.tryload_stage('_upgrade_streamcorpus_v0_3_0', 'upgrade_streamcorpus_v0_3_0')
        self.tryload_stage('_tokenizer', 'nltk_tokenizer')

        # BatchTransform
        self.tryload_stage('_taggers', 'byte_offset_align_labels')
        self.tryload_stage('_taggers', 'char_offset_align_labels')
        #self.tryload_stage('_taggers', 'line_offset_align_labels')
        self.tryload_stage('_taggers', 'name_align_labels')
        self.tryload_stage('_taggers', 'multi_token_match_align_labels')
        self.tryload_stage('_lingpipe', 'lingpipe')
        self.tryload_stage('_serif', 'serif')

        # 'writers' move data out of the pipeline
        self.tryload_stage('_local_storage', 'to_local_chunks')
        self.tryload_stage('_local_storage', 'to_local_tarballs')
        self.tryload_stage('_kvlayer', 'to_kvlayer')
        self.tryload_stage('_s3_storage', 'to_s3_chunks')
        self.tryload_stage('_s3_storage', 'to_s3_tarballs')

        # load from setuptools 'entry_points' of other installed packages
        for entry_point in pkg_resources.iter_entry_points('streamcorpus_pipeline.stages'):
            try:
                name = entry_point.name
                stage_constructor = entry_point.load()
                self[name] = stage_constructor
            except Exception, exc:
                import traceback
                print(traceback.format_exc(exc))
                logger.error('failure loading plugin entry point: %r', entry_point and entry_point.name, exc_info=True)
                logger.error('IGNORING plug loading failure and continuing on...')

        logger.debug('streamcorpus_pipeline.PipelineStages init in %s sec', time.time() - start)
