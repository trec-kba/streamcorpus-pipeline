#!/usr/bin/env python
'''
See help(streamcorpus_pipeline.Pipeline) for details on configuration.

This software is released under an MIT/X11 open source license.

Copyright 2012-2014 Diffeo, Inc.
'''
from __future__ import absolute_import
import copy
import glob
import importlib
import logging
import json
import os
import sys
import time

import dblogger
import kvlayer
from yakonfig import set_global_config, set_runtime_args_object

from streamcorpus_pipeline._exceptions import ConfigurationError
from streamcorpus_pipeline._pipeline import Pipeline

logger = logging.getLogger(__name__)

def make_absolute_paths( config ):
    '''
    given a config dict with streamcorpus_pipeline as a key, find all
    keys under streamcorpus_pipeline that end with "_path" and if the
    value of that key is a relative path, convert it to an absolute
    path using the value provided by root_path
    '''
    if not 'streamcorpus_pipeline' in config:
        logger.critical('bad config: %r', config)
        raise ConfigurationError('missing "streamcorpus_pipeline" from config')
    ## remove the root_path, so it does not get extended itself
    root_path = config['streamcorpus_pipeline'].pop('root_path', None)
    if not root_path:
        root_path = os.getcwd()

    if not root_path.startswith('/'):
        root_path = os.path.join( os.getcwd(), root_path )

    def recursive_abs_path( sub_config, root_path ):
        for key, val in sub_config.items():
            if isinstance(val, basestring):
                if key.endswith('path'):
                    ## we have a path... is it already absolute?
                    if not val.startswith('/'):
                        ## make the path absolute
                        sub_config[key] = os.path.join(root_path, val)

            elif isinstance(val, dict):
                recursive_abs_path( val, root_path )

    recursive_abs_path( config, root_path )

    ## put the root_path back
    config['root_path'] = root_path

def make_hash(obj):
    '''
    Makes a hash from a dictionary, list, tuple or set to any level,
    that contains only other hashable types (including any lists,
    tuples, sets, and dictionaries).  See second answer (not the
    accepted answer):
    http://stackoverflow.com/questions/5884066/hashing-a-python-dictionary
    '''
    if isinstance(obj, (set, tuple, list)):
        return tuple([make_hash(e) for e in obj])
    elif not isinstance(obj, dict):
        return hash(obj)

    new_obj = copy.deepcopy(obj)
    for k, v in new_obj.items():
        ## call self recursively
        new_obj[k] = make_hash(v)

    return hash(tuple(frozenset(new_obj.items())))

def instantiate_config(config):
    '''setup the config and load external modules
    '''
    make_absolute_paths(config)

    pipeline_config = config['streamcorpus_pipeline']

    pipeline_config['config_hash'] = make_hash(config)
    pipeline_config['config_json'] = json.dumps(config)

    ## setup loggers
    if 'log_level' in pipeline_config:
        logging.getLogger().setLevel(pipeline_config['log_level'])

    logger.debug('running config: {} = {!r}'
                 .format(pipeline_config['config_hash'], config))

    ## Load modules
    # This is a method of using settings in yaml configs to load plugins.
    die = False
    for pathstr in pipeline_config.get('pythonpath', {}).itervalues():
        if pathstr not in sys.path:
            sys.path.append(pathstr)
    for modname in pipeline_config.get('setup_modules', {}).itervalues():
        try:
            m = importlib.import_module(modname)
            if not m:
                logger.critical('could not load module %r', modname)
                die = True
                continue
            if hasattr(m, 'setup'):
                m.setup()
                logger.debug('loaded and setup %r', modname)
            else:
                logger.debug('loaded %r', modname)
        except Exception:
            logger.critical('error loading and initting module %r', modname, exc_info=True)
            die = True
    if die:
        sys.exit(1)


def main():
    import argparse
    parser = argparse.ArgumentParser(
        description=Pipeline.__doc__,
        usage='streamcorpus_pipeline config.yaml')
    parser.add_argument('-i', '--input', action='append', 
                        help='file paths to input instead of reading from stdin')
    parser.add_argument('--in-glob', action='append', default=[], help='path glob specifying input files')
    parser.add_argument('config', metavar='config.yaml', help='yakonfig YAML file')

    ## get kvlayer command line arguments
    kvlayer.add_arguments(
        parser, 
        defaults=dict(
            app_name='streamcorpus_pipeline',
            ),
        include_app_name=True,
        include_namespace=True,
        )

    args = parser.parse_args()

    ## use yakonfig to provide a globally accessible configuration
    set_runtime_args_object(args)
    config = set_global_config(args.config)
    dblogger.configure_logging(config)

    ## this modifies the global config, passed by reference
    instantiate_config(config)

    input_paths = []
    if args.in_glob:
        for pattern in args.in_glob:
            input_paths.extend(glob.glob(pattern))
    if args.input:
        input_paths.extend(args.input)
    if not input_paths:
        input_paths = sys.stdin

    ## get a Pipeline instance, so we can use it below
    pipeline = Pipeline(config)

    for i_str in input_paths:
        work_unit = SimpleWorkUnit(i_str.strip())
        work_unit.data['start_chunk_time'] = time.time()
        work_unit.data['start_count'] = 0
        pipeline._process_task(work_unit)

class SimpleWorkUnit(object):
    '''partially duck-typed rejester.WorkUnit that wraps strings from
    stdin and provides only the methods used by the Pipeline

    '''
    def __init__(self, i_str):
        self.key = i_str
        self.data = dict()

    def update(self):
        ## a real WorkUnit would send self.data to the registry and
        ## renew the lease time
        pass

    def terminate(self):
        pass



if __name__ == '__main__':
    main()
