#!/usr/bin/env python
'''Registry of stages for streamcorpus_pipeline.

-----


This software is released under an MIT/X11 open source license.

Copyright 2012-2014 Diffeo, Inc.
'''

from abc import ABCMeta, abstractmethod
from collections import Callable, MutableMapping
import imp
import logging

import yakonfig

logger = logging.getLogger(__name__)

class StageRegistry(MutableMapping):
    '''List of known stages for a pipeline.

    '''
    def __init__(self, *args, **kwargs):
        super(StageRegistry, self).__init__(*args, **kwargs)
        self.stages = {}

    # MutableMapping special methods

    def __getitem__(self, k):
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
            logger.warn('cannot load stage {}: cannot load module {}'
                        .format(name, moduleName), exc_info=exc)
            return

        if not hasattr(mod, functionName):
            logger.warn('cannot load stage {}: module {} missing {}'
                        .format(name, moduleName, functionName))
            return

        self[name] = getattr(mod, functionName)

    def load_external_stages(self, path):
        '''Add external stages from the Python module in 'path'.

        'path' must be a path to a Python module source that contains
        a 'Stages' dictionary, which is a map from stage name to callable.

        :param str path: path to the module file
        '''
        mod = imp.load_source('', path)
        self.update(mod.Stages)

    def init_stage(self, name, config):
        '''Construct and configure a stage from known stages.

        `name` must be the name of one of the stages in this.  `config`
        is the configuration dictionary of the containing object, and its `name`
        member will be passed into the stage constructor.

        :param str name: name of the stage
        :param dict config: parent object configuration
        :return: callable stage
        :raise KeyError: if `name` is not a known stage

        '''
        subconfig = config.get(name, {})
        ctor = self[name]
        return ctor(subconfig)

class Configured(object):
    """A stage that is configured within the streamcorpus_pipeline."""
    def __init__(self, config, *args, **kwargs):
        super(Configured, self).__init__(*args, **kwargs)
        self.config = config

class BatchTransform(Configured):
    '''Transform that acts on a streamcorpus chunk file.
    '''
    __metaclass__ = ABCMeta


    @abstractmethod
    def process_path(self, chunk_path):
        '''
        process streamcorpus chunk file at chunk_path.
        work in place with results at same path (with tempfile and rename if needed)
        '''
        raise NotImplementedError('BatchTransform.process_path not implemented')

    @abstractmethod
    def shutdown(self):
        '''
        gracefully exit the transform immediately, kill any child processes
        '''
        raise NotImplementedError('shutdown is a required method for all BatchTransforms')


class IncrementalTransform(Configured, Callable):
    '''Transform that acts on individual streamcorpus StreamItem objects.'''
    __metaclass__ = ABCMeta
    @abstractmethod
    def process_item(self, stream_item, context):
        '''
        process streamcorpus StreamItem object.
        context dict contains {
          'i_str': path or other work spec returned by task queue
          'data': aux data returned by task queue
        }
        return modified or copy object, or None if item was filtered out.
        '''
        raise NotImplementedError('BatchTransform.process_path not implemented')

    def shutdown(self):
        '''
        gracefully exit the transform immediately, kill any child processes
        '''
        # no-op default implementation.
        # Most IncrementalTransform implementations are pure python and have no associated daemon.
        pass

class PipelineStages(StageRegistry):
    '''The list of stages, preloaded with streamcorpus_pipeline stages.'''
    def __init__(self, *args, **kwargs):
        super(PipelineStages, self).__init__(*args, **kwargs)

        # data source readers (read data from somewhere into pipeline)
        self.tryload_stage('_convert_kba_json', 'convert_kba_json')
        self.tryload_stage('_local_storage', 'from_local_chunks')
        self.tryload_stage('_kvlayer', 'from_kvlayer')
        self.tryload_stage('_s3_storage', 'from_s3_chunks')
        self.tryload_stage('_john_smith', 'john_smith')
        self.tryload_stage('_yaml_files_list', 'yaml_files_list')
        self.tryload_stage('_spinn3r_feed_storage', 'from_spinn3r_feed')

        # StreamItem stages
        # (alphabetical by stage name)
        self.tryload_stage('_clean_html', 'clean_html')
        self.tryload_stage('_clean_visible', 'clean_visible')
        self.tryload_stage('_pdf_to_text', 'pdf_to_text')
        self.tryload_stage('_docx_to_text', 'docx_to_text')
        self.tryload_stage('_filters', 'debug_filter')
        self.tryload_stage('_dedup', 'dedup')
        self.tryload_stage('_dump_label_stats', 'dump_label_stats')
        self.tryload_stage('_filters', 'exclusion_filter')
        self.tryload_stage('_guess_media_type', 'file_type_stats')
        self.tryload_stage('_filters', 'filter_languages')
        self.tryload_stage('_find', 'find')
        self.tryload_stage('_find', 'find_doc_ids')
        self.tryload_stage('_guess_media_type', 'guess_media_type')
        self.tryload_stage('_handle_unconvertible_spinn3r', 'handle_unconvertible_spinn3r')
        self.tryload_stage('_hyperlink_labels', 'hyperlink_labels')
        self.tryload_stage('_upgrade_streamcorpus', 'keep_annotated')
        self.tryload_stage('_upgrade_streamcorpus', 'keep_annotated', 'keep_annotatoted')
        self.tryload_stage('_language', 'language')
        self.tryload_stage('_filters', 'remove_raw')
        self.tryload_stage('_upgrade_streamcorpus', 'upgrade_streamcorpus')
        self.tryload_stage('_upgrade_streamcorpus_v0_3_0', 'upgrade_streamcorpus_v0_3_0')
        self.tryload_stage('_tokenizer', 'nltk_tokenizer')

        # BatchTransform
        self.tryload_stage('_taggers', 'byte_offset_align_labels')
        self.tryload_stage('_taggers', 'line_offset_align_labels')
        self.tryload_stage('_taggers', 'name_align_labels')
        self.tryload_stage('_lingpipe', 'lingpipe')
        self.tryload_stage('_serif', 'serif')
        
        # 'writers' move data out of the pipeline
        self.tryload_stage('_local_storage', 'to_local_chunks')
        self.tryload_stage('_local_storage', 'to_local_tarballs')
        self.tryload_stage('_kvlayer', 'to_kvlayer')
        self.tryload_stage('_s3_storage', 'to_s3_chunks')
        self.tryload_stage('_s3_storage', 'to_s3_tarballs')
