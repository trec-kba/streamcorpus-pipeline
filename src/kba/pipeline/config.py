#!/usr/bin/python
'''
See help(kba) for details on configuring a kba.pipeline

This software is released under an MIT/X11 open source license.

Copyright 2012-2013 Diffeo, Inc.
'''
import os
import sys
import yaml
from _exceptions import ConfigurationError

import logging
log_level = 'DEBUG'
logger = logging.getLogger('kba')
logger.setLevel( log_level )
ch = logging.StreamHandler()
ch.setLevel( log_level )
#ch.setFormatter(formatter)
logger.addHandler(ch)

possible_root_paths = [os.getcwd(), os.path.join(os.getcwd(), '..'),
                       '/opt/diffeo']

config_file_name = 'configs/diffeo.yaml'
default_config = None
for root_path in possible_root_paths:
    default_config_path = os.path.join(root_path, config_file_name)
    if not os.path.exists(default_config_path):
        logger.info('failed to find %s, so trying next location' % default_config_path)
        continue
    ## might have several config blocks, e.g. treelab, kba, bigtree
    full_config = yaml.load(open(default_config_path))
    default_config = full_config.get('kba.pipeline', {}).get('zookeeper', {})

if not default_config:
    msg = 'failed to find %s in %r' % (config_file_name, possible_root_paths)
    logger.critical(msg)
    sys.exit(msg)

def get_config(**kwargs):
    '''
    convert kwargs into a valid config dict by filling in missing
    values from default_config
    '''
    config = {}
    config.update(default_config)
    ## cherry pick only the config params that we know about
    for key in config:
        if key in kwargs:
            config[key] = kwargs[key]
    validate_config(config)
    return config

def validate_config(config):
    '''
    raise ConfigurationError if config is invalid or incomplete
    '''
    namespace = config.get('namespace', None)
    if namespace is None or len(config['namespace']) == 0:
        raise ConfigurationError(
            "config['namespace']=%r must be defined" % namespace)

def add_command_line_args(parser):
    '''
    add bigtree-specific commandline arguments to a parser.  This can
    be called by an application using bigtree, e.g. treelab
    '''
    parser.add_argument('--storage-type')
    parser.add_argument('--registry-type')
    parser.add_argument('--storage-hosts')
    parser.add_argument('--registry-hosts')
    parser.add_argument('--namespace')

def command_line_to_kwargs(parser):
    '''
    convert a command line ArgumentParser object into a dict suitable
    for passing to get_config
    '''
    args = parser.parse_args() 
    kwargs = vars(args)
    ## filter empty values
    kwargs = {key:value for key, value in kwargs.items() if value}
    return kwargs

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(
        description='',
        usage='python -m kba.pipeline.config')

    add_command_line_args(parser)

    kwargs = command_line_to_kwargs(parser)

    config = get_config(**kwargs)

    print yaml.dump( config )
