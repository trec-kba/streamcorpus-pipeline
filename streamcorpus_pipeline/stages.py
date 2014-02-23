#!/usr/bin/env python
'''
This software is released under an MIT/X11 open source license.

Copyright 2012-2013 Diffeo, Inc.
'''

from abc import ABCMeta, abstractmethod
import logging
import threading

import pkg_resources

import yakonfig

logger = logging.getLogger(__name__)

class Configured(object):
    """A stage that is configured within the streamcorpus_pipeline."""
    def __init__(self):
        self.config = yakonfig.get_global_config('streamcorpus_pipeline',
                                                 self.config_name)

class BatchTransform(Configured):
    __metaclass__ = ABCMeta
    '''
    Transform that acts on a streamcorpus chunk file.
    '''

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


# TODO: there are no current implementations of IncrementalTransform
# This is a proposed interface for the near future -- bolson 2013-10-03
#
# Current incremental transform implementations are a function which
# takes (config), possibly a constructor, which returns a callable
# (lambda, local function, __call__ method) which takes (stream_item, context)
class IncrementalTransform(Configured):
    __metaclass__ = ABCMeta
    '''
    Transform that acts on streamcorpus StreamItem objects.
    '''
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


# map from stage name to constructor for stage.
# stage constructor should take a config dict and return an callable of appropriate signature.
# TODO? Separate collections of readers/StreamItem/Chunk file/writer operations?
Stages = {}


def register_stage(name, constructor):
    load_default_stages()
    Stages[name] = constructor

def get_stage(name):
    return Stages[name]

def _tryload_stage(moduleName, functionName, name=None):
    "If loading a module fails because of some subordinate load fail just warn and move on"
    # e.g. if someone doesn't have boto installed and doesn't care about s3 storage, that should be fine
    logger.debug('_tryload_stage(%r, %r, %r)', moduleName, functionName, name)
    try:
        x = __import__(moduleName, globals(), locals(), [functionName])
        if name is None:
            name = functionName
        #logger.debug('_tryload __import__ got %r', x)
        if not hasattr(x, functionName):
            logger.error('module %r missing function %r', moduleName, functionName)
            return
        # this would be theoretically cleaner, but there's a nasty reentrency that way I want to skip
        #register_stage(name, getattr(x, functionName))
        Stages[name] = getattr(x, functionName)
        #logger.debug('_tryload successfully registered stage %r', name)
    except:
        logger.warn('failed on "from %r load %r" stage load', moduleName, functionName, exc_info=True)


_default_stages_loaded = []
_load_lock = threading.Lock()


def load_default_stages():
    if _default_stages_loaded:
        return
    with _load_lock:
        if _default_stages_loaded:
            return

        # data source readers (read data from somewhere into pipeline)
        _tryload_stage('_convert_kba_json', 'convert_kba_json')
        _tryload_stage('_local_storage', 'from_local_chunks')
        _tryload_stage('_kvlayer', 'from_kvlayer')
        _tryload_stage('_s3_storage', 'from_s3_chunks')
        _tryload_stage('_john_smith', 'john_smith')
        _tryload_stage('_yaml_files_list', 'yaml_files_list')
        _tryload_stage('_spinn3r_feed_storage', 'from_spinn3r_feed')

        # StreamItem stages
        # (alphabetical by stage name)
        _tryload_stage('_clean_html', 'clean_html')
        _tryload_stage('_clean_visible', 'clean_visible')
        _tryload_stage('_pdf_to_text', 'pdf_to_text')
        _tryload_stage('_docx_to_text', 'docx_to_text')
        _tryload_stage('_filters', 'debug_filter')
        _tryload_stage('_dedup', 'dedup')
        _tryload_stage('_dump_label_stats', 'dump_label_stats')
        _tryload_stage('_filters', 'exclusion_filter')
        _tryload_stage('_guess_media_type', 'file_type_stats')
        _tryload_stage('_filters', 'filter_languages')
        _tryload_stage('_find', 'find')
        _tryload_stage('_find', 'find_doc_ids')
        _tryload_stage('_guess_media_type', 'guess_media_type')
        _tryload_stage('_handle_unconvertible_spinn3r', 'handle_unconvertible_spinn3r')
        _tryload_stage('_hyperlink_labels', 'hyperlink_labels')
        _tryload_stage('_upgrade_streamcorpus', 'keep_annotated')
        _tryload_stage('_upgrade_streamcorpus', 'keep_annotated', 'keep_annotatoted')
        _tryload_stage('_language', 'language')
        _tryload_stage('_filters', 'remove_raw')
        _tryload_stage('_upgrade_streamcorpus', 'upgrade_streamcorpus')
        _tryload_stage('_upgrade_streamcorpus_v0_3_0', 'upgrade_streamcorpus_v0_3_0')
        _tryload_stage('_tokenizer', 'nltk_tokenizer')

        # BatchTransform
        _tryload_stage('_taggers', 'byte_offset_align_labels')
        _tryload_stage('_taggers', 'line_offset_align_labels')
        _tryload_stage('_taggers', 'name_align_labels')
        _tryload_stage('_lingpipe', 'lingpipe')
        _tryload_stage('_serif', 'serif')

        # 'writers' move data out of the pipeline
        _tryload_stage('_local_storage', 'to_local_chunks')
        _tryload_stage('_local_storage', 'to_local_tarballs')
        _tryload_stage('_kvlayer', 'to_kvlayer')
        _tryload_stage('_s3_storage', 'to_s3_chunks')
        _tryload_stage('_s3_storage', 'to_s3_tarballs')

        _default_stages_loaded.append(len(_default_stages_loaded) + 1)

def load_external_stages(path):
    """Add external stages from the Python module in 'path'.

    'path' must be a path to a Python module source that contains
    a 'Stages' dictionary, which is a map from stage name to callable.

    """
    load_default_stages()
    import imp
    mod = imp.load_source('', path)
    Stages.update(mod.Stages)

def _init_stage(name):
    '''
    Construct a stage from known Stages.

    :param name: string name of a stage in Stages

    :returns callable: one of four possible types:

       1) readers: take byte strings as input and emit StreamItems

       2) incremental transforms: take StreamItem and emit StreamItem
       
       3) batch transforms: take Chunk and emit Chunk

       4) writers: take Chunk and push it somewhere
    '''
    load_default_stages()

    stage_constructor = Stages[name]
    stage = stage_constructor()

    return stage
