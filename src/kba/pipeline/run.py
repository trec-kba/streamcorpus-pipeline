#!/usr/bin/python
'''
See help(Pipeline) for details on configuring a Pipeline.

This software is released under an MIT/X11 open source license.

Copyright 2012 Diffeo, Inc.
'''
import os
import sys
import copy
import json
import logging
from _pipeline import Pipeline

def make_absolute_paths( config ):
    ## remove the root_path, so it does not get extended itself
    root_path = config['kba.pipeline'].pop('root_path', None)
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

if __name__ == '__main__':
    import yaml
    import argparse
    parser = argparse.ArgumentParser(
        description=Pipeline.__doc__,
        usage='python -m kba.pipeline.run config.yaml')
    parser.add_argument('config', metavar='config.yaml', 
                        help='configuration parameters for a pipeline run')
    args = parser.parse_args()

    if not args.config.startswith('/'):
        args.config = os.path.join(os.getcwd(), args.config)

    assert os.path.exists(args.config), '%s does not exist' % args.config
    config = yaml.load(open(args.config))

    make_absolute_paths(config)

    print 'loaded config from %r' % args.config

    ## put info about the whole config in the extractor's config
    tq_name = config['kba.pipeline'].get('task_queue', 'no-task-queue')
    if tq_name not in config['kba.pipeline'] or config['kba.pipeline'][tq_name] is None:
        config['kba.pipeline'][tq_name] = {}
    config['kba.pipeline'][tq_name]['config_hash'] = make_hash(config)
    config['kba.pipeline'][tq_name]['config_json'] = json.dumps(config)

    ## setup loggers
    log_level = getattr(logging, config['kba.pipeline']['log_level'])

    logger = logging.getLogger('kba')
    logger.setLevel( log_level )

    ch = logging.StreamHandler()
    ch.setLevel( log_level )
    formatter = logging.Formatter('%(asctime)s %(process)d %(levelname)s: %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    logger.critical('running config: %s = %s' % (
            config['kba.pipeline'][tq_name]['config_hash'], args.config))

    pipeline = Pipeline(config)
    pipeline.run()

