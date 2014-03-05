"""yakonfig declarations for streamcorpus_pipeline.

-----

This software is released under an MIT/X11 open source license.

Copyright 2014 Diffeo, Inc.

"""
from __future__ import absolute_import
import collections
import imp

import streamcorpus_pipeline
import streamcorpus_pipeline.stages as stages
from yakonfig import ConfigurationError, check_subconfig, NewSubModules

config_name = 'streamcorpus_pipeline'
default_config = {
    'output_chunk_max_count': 500,
    'rate_log_interval': 100,
    'incremental_transforms': [],
    'batch_transforms': [],
    'post_batch_incremental_transforms': [],
}
stages.load_default_stages()
sub_modules = set(stage
                  for stage in stages.Stages.itervalues()
                  if hasattr(stage, 'config_name'))

def replace_config(config, name):
    # Do we have external stages?
    if 'external_stages_path' not in config: return streamcorpus_pipeline
    try:
        stages.load_external_stages(config['external_stages_path'])
    except IOError, e:
        return streamcorpus_pipeline # let check_config re-raise this
    # ugh
    new_sub_modules = set(stage
                          for stage in stages.Stages.itervalues()
                          if hasattr(stage, 'config_name'))
    return NewSubModules(streamcorpus_pipeline, new_sub_modules)

def check_config(config, name):
    if 'tmp_dir_path' not in config:
        raise ConfigurationError('{} requires tmp_dir_path setting'
                                 .format(name))

    # Checking stages:
    # (1) Push in the external stages; 
    if 'external_stages_path' in config:
        try:
            stages.load_external_stages(config['external_stages_path'])
        except IOError, e:
            raise ConfigurationError(
                'invalid {} external_stages_path {}'
                .format(name, config['external_stages_path']), e)

    # (2) Check the reader;
    if 'reader' not in config:
        raise ConfigurationError('{} requires reader stage'
                                 .format(name))
    try:
        reader = stages.get_stage(config['reader'])
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
                stage = stages.get_stage(stagename)
            except ValueError, e:
                raise ConfigurationError(
                    'invalid {} {} {}'
                    .format(name, phase, stagename))
            check_subconfig(config, name, stage)
