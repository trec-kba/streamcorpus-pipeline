#!/usr/bin/env python
'''
This software is released under an MIT/X11 open source license.

Copyright 2012-2013 Diffeo, Inc.
'''

from abc import ABCMeta, abstractmethod
import logging
import threading

import pkg_resources

logger = logging.getLogger(__name__)

class BatchTransform(object):
    __metaclass__ = ABCMeta
    '''
    Transform that acts on a streamcorpus chunk file.
    '''

    def __init__(self, config):
        self.config = config

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
class IncrementalTransform(object):
    __metaclass__ = ABCMeta
    '''
    Transform that acts on streamcorpus StreamItem objects.
    '''
    def __init__(self, config):
        self.config = config

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
# TODO? Separate collections of extractors/StreamItem/Chunk file/loader operations?
Stages = {}


def register_stage(name, constructor):
    _load_default_stages()
    Stages[name] = constructor


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


def _load_default_stages():
    if _default_stages_loaded:
        return
    with _load_lock:
        if _default_stages_loaded:
            return

        # data source extractors (read data from somewhere into pipeline)
        _tryload_stage('_convert_kba_json', 'convert_kba_json')
        _tryload_stage('_local_storage', 'from_local_chunks')
        _tryload_stage('_kvlayer', 'from_kvlayer')
        _tryload_stage('_s3_storage', 'from_s3_chunks')
        _tryload_stage('_john_smith', 'john_smith')

        # StreamItem stages
        # (alphabetical by stage name)
        _tryload_stage('_clean_html', 'clean_html')
        _tryload_stage('_clean_visible', 'clean_visible')
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
        _tryload_stage('_upgrade_streamcorpus', 'keep_annotatoted', 'keep_annotated')
        _tryload_stage('_upgrade_streamcorpus', 'keep_annotatoted', 'keep_annotatoted')
        _tryload_stage('_language', 'language')
        _tryload_stage('_filters', 'remove_raw')
        _tryload_stage('_upgrade_streamcorpus', 'upgrade_streamcorpus')
        _tryload_stage('_upgrade_streamcorpus_v0_3_0', 'upgrade_streamcorpus_v0_3_0')

        # BatchTransform
        _tryload_stage('_taggers', 'byte_offset_align_labels')
        _tryload_stage('_taggers', 'line_offset_align_labels')
        _tryload_stage('_taggers', 'name_align_labels')

        # 'loaders' move data out of the pipeline
        _tryload_stage('_local_storage', 'to_local_chunks')
        _tryload_stage('_local_storage', 'to_local_tarballs')
        _tryload_stage('_kvlayer', 'to_kvlayer')
        _tryload_stage('_s3_storage', 'to_s3_chunks')
        _tryload_stage('_s3_storage', 'to_s3_tarballs')

        _default_stages_loaded.append(len(_default_stages_loaded) + 1)


def _init_stage(name, config, external_stages=None):
    '''
    Construct a stage from known Stages.

    :param name: string name of a stage in Stages

    :param config: config dict passed into the stage constructor

    :returns callable: one of four possible types:

       1) extractors: take byte strings as input and emit StreamItems

       2) incremental transforms: take StreamItem and emit StreamItem
       
       3) batch transforms: take Chunk and emit Chunk

       4) loaders: take Chunk and push it somewhere
    '''
    _load_default_stages()

    if external_stages:
        Stages.update( external_stages )

    stage_constructor = Stages.get(name, None)
    if stage_constructor is None:
        #stage_constructor = pkg_resources.load_entry_point('streamcorpus.pipeline.stages', name)
        entries = pkg_resources.iter_entry_points('streamcorpus.pipeline.stages', name)
        entries = list(entries)
        if not entries:
            pass
        elif len(entries) > 1:
            logger.error('multiple entry_points for pipeline stage %r: %r', name, entries)
        else:
            stage_constructor = entries[0].load()
        if stage_constructor is not None:
            Stages[name] = stage_constructor
    if stage_constructor is None:
        raise Exception('unknown stage %r' % (name,))
    stage = stage_constructor(config)

    ## NB: we don't mess with config here, because even though the
    ## usual usage involves just passing in config.get(name, {}),
    ## there might be callers that need to modify config on the way in

    # if using __import__()
    ## Note that fromlist must be specified here to cause __import__()
    ## to return the right-most component of name, which in our case
    ## must be a function.  The contents of fromlist is not
    ## considered; it just cannot be empty:
    ## http://stackoverflow.com/questions/2724260/why-does-pythons-import-require-fromlist
    #trans = __import__('clean_html', fromlist=['streamcorpus.pipeline'])

    return stage
