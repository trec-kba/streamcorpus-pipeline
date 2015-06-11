"""yakonfig declarations for streamcorpus_pipeline.

.. This software is released under an MIT/X11 open source license.
   Copyright 2014-2015 Diffeo, Inc.

If your program will use :mod:`streamcorpus_pipeline`, then include
the top-level module in your initial :mod:`yakonfig` setup, e.g.

.. code-block:: python

   with yakonfig.parse_args(parser, [streamcorpus_pipeline]):
     ...

.. todo:: Replace
          :class:`streamcorpus_pipeline._pipeline.PipelineFactory`
          with a :class:`yakonfig.factory.AutoFactory` implementation,
          which will mean that callers will need to pass a factory
          object to :mod:`yakonfig`.

.. todo:: Stop forcibly modifying stage configuration; but in turn
          figure out what we want the semantics of relative paths
          in the configuration to be.

"""
from __future__ import absolute_import
import collections
import logging
import os

import streamcorpus_pipeline
from streamcorpus_pipeline.stages import PipelineStages
from yakonfig import ConfigurationError, check_subconfig, NewSubModules

logger = logging.getLogger(__name__)

config_name = 'streamcorpus_pipeline'
default_config = {
    'tmp_dir_path': '/tmp',
    'third_dir_path': '/',
    'output_chunk_max_count': 500,
    'rate_log_interval': 100,
    'incremental_transforms': [],
    'batch_transforms': [],
    'post_batch_incremental_transforms': [],
    'cleanup_tmp_files': True,
    'assert_single_source': True,
    'reader': 'from_local_chunks',
    'writers': ['to_local_chunks'],
}
runtime_keys = {
    'tmp_dir_path': 'tmp_dir_path',
    'third_dir_path': 'third_dir_path',
}
# We can't find out our sub-modules just yet!  Since this gets evaluated
# at module import time, if an entrypoint stage tries to import
# streamcorpus_pipeline it can go wrong.
sub_modules = set()


static_stages = None

def replace_config(config, name):
    '''Replace the top-level pipeline configurable object.

    This investigates a number of sources, including
    `external_stages_path` and `external_stages_modules` configuration
    and `streamcorpus_pipeline.stages` entry points, and uses these to
    find the actual :data:`sub_modules` for
    :mod:`streamcorpus_pipeline`.

    '''
    global static_stages
    if static_stages is None:
        static_stages = PipelineStages()
        stages = static_stages
        if 'external_stages_path' in config:
            path = config['external_stages_path']
            if not os.path.isabs(path) and config.get('root_path'):
                path = os.path.join(config['root_path'], path)
            try:
                stages.load_external_stages(config['external_stages_path'])
            except IOError:
                return streamcorpus_pipeline  # let check_config re-raise this
        if 'external_stages_modules' in config:
            for mod in config['external_stages_modules']:
                try:
                    stages.load_module_stages(mod)
                except ImportError:
                    return streamcorpus_pipeline  # let check_config re-raise this
    else:
        stages = static_stages

    new_sub_modules = set(stage
                          for stage in stages.itervalues()
                          if hasattr(stage, 'config_name'))
    return NewSubModules(streamcorpus_pipeline, new_sub_modules)


def check_config(config, name):
    if 'tmp_dir_path' not in config:
        raise ConfigurationError('{0} requires tmp_dir_path setting'
                                 .format(name))

    # Checking stages:
    global static_stages
    if static_stages is None:
        static_stages = PipelineStages()
    stages = static_stages

    # (1) Push in the external stages;
    if 'external_stages_path' in config:
        try:
            stages.load_external_stages(config['external_stages_path'])
        except IOError, e:
            raise ConfigurationError(
                'invalid {0} external_stages_path {1}'
                .format(name, config['external_stages_path']), e)
    if 'external_stages_modules' in config:
            for mod in config['external_stages_modules']:
                try:
                    stages.load_module_stages(mod)
                except ImportError, e:
                    raise ConfigurationError(
                        'invalid {0} external_stages_modules value {1}'
                        .format(name, mod), e)

    # (2) Check the reader;
    if 'reader' not in config:
        raise ConfigurationError('{0} requires reader stage'
                                 .format(name))
    try:
        reader = stages[config['reader']]
    except ValueError, e:
        raise ConfigurationError(
            'invalid {0} reader {1}'
            .format(name, config['reader']))
    check_subconfig(config, name, reader)

    # (3) Check all of the intermediate and writers
    for phase in ['incremental_transforms', 'batch_transforms',
                  'post_batch_incremental_transforms', 'writers']:
        if phase not in config:
            raise ConfigurationError('{0} requires {1} stage list'
                                     .format(name, phase))
        if not isinstance(config[phase], collections.Iterable):
            raise ConfigurationError('{0} {1} must be a list of stages'
                                     .format(name, phase))
        for stagename in config[phase]:
            try:
                stage = stages[stagename]
            except KeyError, e:
                raise ConfigurationError(
                    'invalid {0} {1} {2}'
                    .format(name, phase, stagename))
            check_subconfig(config, name, stage)


def normalize_config(config):
    # Fix up all paths in our own config to be absolute
    root_path = config.get('root_path', os.getcwd())

    def fix(c, k):
        v = c[k]
        if v is None:
            return
        if isinstance(v, list):
            c[k] = [os.path.join(root_path, p) for p in v]
        if not os.path.isabs(v):
            c[k] = os.path.join(root_path, v)

    for k in config.iterkeys():
        if k.endswith('path') and k != 'root_path':
            fix(config, k)

    # Now go into all of our children and fix up their paths too.
    for c in config.itervalues():
        if isinstance(c, collections.MutableMapping):
            for k in c.iterkeys():
                if k.endswith('path'):
                    fix(c, k)
