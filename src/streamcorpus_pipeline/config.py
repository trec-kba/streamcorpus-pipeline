#!/usr/bin/python
'''
This software is released under an MIT/X11 open source license.

Copyright 2012-2013 Diffeo, Inc.
'''

import collections
import json
import os
import sys
import yaml
from _exceptions import ConfigurationError

import pdb

from ._logging import logger, configure_logger

possible_root_paths = [
    os.getcwd(),
    os.path.abspath(os.path.join(os.getcwd(), '..')),
    # if this file is src/streamcorpus_pipline/config.py then ../../configs/*yaml
    os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')),
    '/opt/diffeo'
]

config_file_name = 'configs/defaults.yaml'


_global_config = None
_global_config_source = None


def _try_load_config(default_config_path):
    if not os.path.exists(default_config_path):
        #logger.info('failed to find %s, so trying next location' % default_config_path)
        return None
    #logger.info('found %r', default_config_path)
    full_config = yaml.load(open(default_config_path))
    # grab just our block, other parts of the system could use different sections of the config yaml
    return full_config.get('streamcorpus.pipeline', None)


def path_load_config(path=None, filename='configs/defaults.yaml'):
    '''
    @type path: array of directory path strings, e.g. ['/etc', '/opt']
    @type filename: suffix which can be os.path.join()ed to path strings
    @rtype: {config: dict}, 'path of loaded config', or (None,None)
    '''
    default_config = None
    if path is None:
        path = possible_root_paths
    for root_path in path:
        default_config_path = os.path.join(root_path, filename)
        default_config = _try_load_config(default_config_path)
        if default_config is not None:
            # successfully got _something_
            logger.info('read config from %r', default_config_path)
            return default_config, default_config_path
    return None, None


def get_global_config(filename=None):
    global _global_config
    global _global_config_source
    if _global_config is not None:
        return _global_config
    if filename is None:
        filename = config_file_name

    default_config, default_config_path = path_load_config(possible_root_paths, filename)
    if default_config is not None:
        _global_config = default_config
        _global_config_source = default_config_path
        return _global_config

    if not default_config:
        msg = 'failed to find %s in %r' % (filename, possible_root_paths)
        logger.critical(msg)
        sys.exit(msg)
    
    return None


def deep_update(d, u, existing_keys=False, ignore_none=False):
    '''
    recursively updates a tree of dict objects, selectively changing
    or adding only those fields that appear in "u", which is the
    update argument.

    If existing_keys is set to True, then only keys that are already
    in "d" will be considered for modification.

    if ignore_none is set to True, then values that are None are
    ignored.
    '''
    try:
        for k, v in u.iteritems():
            if existing_keys and k not in d:
                continue
            if isinstance(v, collections.Mapping):
                r = deep_update(d.get(k, {}), v)
                d[k] = r
            else:
                if ignore_none and v is None:
                    continue
                d[k] = v
    except Exception, exc:
        msg = 'FAILURE:  deep_update(%r, %r)' % \
              (json.dumps(d, indent=4, sort_keys=True),
               json.dumps(u, indent=4, sort_keys=True))
        logger.critical(msg, exc_info=True)
        sys.exit(msg)
    return d


def load_layered_configs(configlist):
    "Read listed configs, layer them (later values win), return unified config dict"
    config = {}
    for configname in configlist:
        tconf = yaml.load(open(configname, 'r'))
        deep_update(config, tconf)
    return config


def config_to_string(config):
    return yaml.dump(config)


def get_config(**kwargs):
    '''
    convert kwargs into a valid config dict by filling in missing
    values from default_config
    '''
    config = {}
    config.update(get_global_config())
    config = deep_update(config, kwargs)
    validate_config(config)
    return config

def validate_config(config):
    '''
    raise ConfigurationError if config is invalid or incomplete
    '''
    pass
    # TODO: figure out what parts of the config are really globally needed for any streamcorpus.pipeline operation
#    namespace = config.get('namespace', None)
#    if namespace is None or len(config['namespace']) == 0:
#        raise ConfigurationError(
#            "config['namespace']=%r must be defined" % namespace)

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
