#!python

import logging

logger = logging.getLogger(__name__)

class BatchTransform(object):
    def shutdown(self):
        '''
        gracefully exit the transform immediately, kill any child processes
        '''
        raise NotImplementedError('shutdown is a required method for all BatchTransforms')


# map from stage name to constructor for stage.
# stage constructor should take a config dict and return an callable of appropriate signature.
# TODO? Separate collections of extractors/StreamItem/Chunk file/loader operations?
Stages = {}


def register_stage(name, constructor):
    Stages[name] = constructor


def _tryload_stage(moduleName, functionName, name=None):
    "If loading a module fails because of some subordinate load fail (package not available) move on"
    try:
        x = __import__(moduleName, globals(), locals(), [functionName])
        if name is None:
            name = functionName
        register_stage(name, getattr(x, functionName))
    except:
        logger.warn('failed on "from %r load %r" stage load', moduleName, functionName, exc_info=True)

# task queues
_tryload_stage('_task_queues', 'stdin')
_tryload_stage('_task_queues', 'itertq')

# data source extractors
_tryload_stage('_local_storage', 'from_local_chunks')

# StreamItem stages
_tryload_stage('_clean_html', 'clean_html')
_tryload_stage('_clean_visible', 'clean_visible')
_tryload_stage('_dedup', 'dedup')
_tryload_stage('_hyperlink_labels', 'hyperlink_labels')
_tryload_stage('_language', 'language')
_tryload_stage('_upgrade_streamcorpus_v0_3_0', 'upgrade_streamcorpus_v0_3_0')

# 'loaders' move data out of the pipeline
_tryload_stage('_local_storage', 'to_local_chunks')



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
    if external_stages:
        Stages.update( external_stages )

    stage_constructor = Stages.get(name, None)
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
    #trans = __import__('clean_html', fromlist=['kba.pipeline'])

    return stage
