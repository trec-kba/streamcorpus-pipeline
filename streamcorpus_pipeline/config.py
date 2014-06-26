"""yakonfig declarations for streamcorpus_pipeline.

.. This software is released under an MIT/X11 open source license.
   Copyright 2014 Diffeo, Inc.

"""
from __future__ import absolute_import
import collections
import imp
import logging
import os
import uuid

import streamcorpus_pipeline
from streamcorpus_pipeline.stages import PipelineStages
from yakonfig import ConfigurationError, check_subconfig, NewSubModules

logger = logging.getLogger(__name__)

config_name = 'streamcorpus_pipeline'
default_config = {
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
sub_modules = set(stage
                  for stage in PipelineStages().itervalues()
                  if hasattr(stage, 'config_name'))

def replace_config(config, name):
    # Do we have external stages?
    if ('external_stages_path' not in config and
        'external_stages_modules' not in config):
        return streamcorpus_pipeline
    stages = PipelineStages()
    if 'external_stages_path' in config:
        path = config['external_stages_path']
        if not os.path.isabs(path) and config.get('root_path'):
            path = os.path.join(config['root_path'], path)
        try:
            stages.load_external_stages(config['external_stages_path'])
        except IOError, e:
            return streamcorpus_pipeline # let check_config re-raise this
    if 'external_stages_modules' in config:
        for mod in config['external_stages_modules']:
            try:
                stages.load_module_stages(mod)
            except ImportError, e:
                return streamcorpus_pipeline # let check_config re-raise this
    new_sub_modules = set(stage
                          for stage in stages.itervalues()
                          if hasattr(stage, 'config_name'))
    return NewSubModules(streamcorpus_pipeline, new_sub_modules)

def check_config(config, name):
    if 'tmp_dir_path' not in config:
        raise ConfigurationError('{} requires tmp_dir_path setting'
                                 .format(name))

    # Checking stages:
    stages = PipelineStages()

    # (1) Push in the external stages; 
    if 'external_stages_path' in config:
        try:
            stages.load_external_stages(config['external_stages_path'])
        except IOError, e:
            raise ConfigurationError(
                'invalid {} external_stages_path {}'
                .format(name, config['external_stages_path']), e)
    if 'external_stages_modules' in config:
            for mod in config['external_stages_modules']:
                try:
                    stages.load_module_stages(mod)
                except ImportError, e:
                    raise ConfigurationError(
                        'invalid {} external_stages_modules value {}'
                        .format(name, mod), e)

    # (2) Check the reader;
    if 'reader' not in config:
        raise ConfigurationError('{} requires reader stage'
                                 .format(name))
    try:
        reader = stages[config['reader']]
    except ValueError, e:
        raise ConfigurationError(
            'invalid {} reader {}'
            .format(name, config['reader']))
    check_subconfig(config, name, reader)

    # (3) Check all of the intermediate and writers
    for phase in ['incremental_transforms', 'batch_transforms', 
                  'post_batch_incremental_transforms', 'writers']:
        if phase not in config:
            raise ConfigurationError('{} requires {} stage list'
                                     .format(name, phase))
        if not isinstance(config[phase], collections.Iterable):
            raise ConfigurationError('{} {} must be a list of stages'
                                     .format(name, phase))
        for stagename in config[phase]:
            try:
                stage = stages[stagename]
            except ValueError, e:
                raise ConfigurationError(
                    'invalid {} {} {}'
                    .format(name, phase, stagename))
            check_subconfig(config, name, stage)

def normalize_config(config):
    # Fix up all paths in our own config to be absolute
    root_path = config.get('root_path', os.getcwd())
    def fix(c, k):
        v = c[k]
        if v is None:
            return
        if not os.path.isabs(v):
            c[k] = os.path.join(root_path, v)
    for k in config.iterkeys():
        if k.endswith('path') and k != 'root_path': fix(config, k)

    # Note, that happens to also include tmp_dir_path, which must exist.

    # ensure that tmp_dir_path is unique to this process, so Pipeline
    # can remove it and anything left in it by various stages.
    config['tmp_dir_path'] = os.path.join(config['tmp_dir_path'], uuid.uuid4().hex)

    tmp_dir_path = config['tmp_dir_path']
    logger.debug('tmp_dir_path --> %s', tmp_dir_path)

    third_dir_path = config.get('third_dir_path')
    # Now go into all of our children and push in tmp_dir_path and fix
    # up their paths too.
    for c in config.itervalues():
        if isinstance(c, collections.MutableMapping):
            for k in c.iterkeys():
                if k.endswith('path'): fix(c, k)
            c['tmp_dir_path'] = tmp_dir_path
            c['third_dir_path'] = third_dir_path
