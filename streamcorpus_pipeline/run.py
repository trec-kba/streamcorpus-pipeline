#!/usr/bin/env python
''':command:`streamcorpus_pipeline` is the command-line entry point to the
pipeline.

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2014 Diffeo, Inc.

.. program:: streamcorpus_pipeline

The supplied configuration must have a :mod:`streamcorpus_pipeline`
section.  Other required configuration depends on the specific stages
in use and the rest of the operating environment; for instance, if the
:class:`~streamcorpus_pipeline._kvlayer.to_kvlayer` writer stage is in
use, then the configuration must also contain a top-level
:mod:`kvlayer` section.

This supports the standard :option:`--config <yakonfig --config>`,
:option:`--dump-config <yakonfig --dump-config>`, :option:`--verbose
<dblogger --verbose>`, :option:`--quiet <dblogger --quiet>`, and
:option:`--debug <dblogger --debug>` options.  It supports the
following additional options:

.. option:: --input <file.sc>, -i <file.sc>

   Names the input file.  If the file name is ``-`` then standard
   input is used, if this makes sense.  Some reader stages may use the
   file name specially.

.. option:: --in-glob <pattern>

   Runs the pipeline once for each file matching the shell :mod:`glob`
   `pattern`.

'''
from __future__ import absolute_import
import copy
import glob
import importlib
import itertools
import logging
import json
import os
import re
import sys
import time

import yaml

import dblogger
import kvlayer
import yakonfig
from yakonfig.toplevel import assemble_default_config

import streamcorpus_pipeline
from streamcorpus_pipeline._exceptions import ConfigurationError
from streamcorpus_pipeline._pipeline import PipelineFactory, Pipeline
from streamcorpus_pipeline.stages import PipelineStages

logger = logging.getLogger(__name__)

def make_absolute_paths(config):
    '''given a config dict with streamcorpus_pipeline as a key, find all
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
                    ## ignore URLs in *_path parameters
                    if re.match('^http.?://', val): continue
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

    This updates 'config' as follows:

    * All paths are replaced with absolute paths
    * A hash and JSON dump of the config are stored in the config
    * If 'pythonpath' is in the config, it is added to sys.path
    * If 'setup_modules' is in the config, all modules named in it are loaded
    '''
    make_absolute_paths(config)

    pipeline_config = config['streamcorpus_pipeline']

    pipeline_config['config_hash'] = make_hash(config)
    pipeline_config['config_json'] = json.dumps(config)

    logger.debug('running config: {0} = {1!r}'
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


def pathfile_iter(path):
    if path.endswith('.gz'):
        fin = gzip.open(path, 'rb')
    else:
        fin = open(path, 'rb')
    for line in fin:
        line = line.strip()
        yield line


def main():
    import argparse
    parser = argparse.ArgumentParser(
        description='process a sequence of stream items',
        usage='streamcorpus_pipeline --config config.yaml --input file.in')
    parser.add_argument('-i', '--input', action='append',
                        help='file paths to input instead of reading from stdin')
    parser.add_argument('--in-glob', action='append', default=[], help='path glob specifying input files')
    parser.add_argument('--third-dir-path', help='path to third-party tools directory')
    parser.add_argument('--tmp-dir-path', help='path to temporary directory for scratch files, can be large')
    parser.add_argument('-f', '--file-of-paths', dest='file_of_paths', default=None, help='path to file with list of paths for input, one per line')
    parser.add_argument('--skip', type=int, default=0,
                        help='Skip the first N stream items.')

    modules = [yakonfig, kvlayer, dblogger, streamcorpus_pipeline]
    args = yakonfig.parse_args(parser, modules)
    config = yakonfig.get_global_config()

    ## this modifies the global config, passed by reference
    instantiate_config(config)

    input_paths = []
    if args.in_glob:
        for pattern in args.in_glob:
            input_paths.extend(glob.glob(pattern))
    if args.input:
        if '-' in args.input:
            if args.in_glob:
                sys.exit('cannot use "-i -" and --in-glob together')
            if len(args.input) > 1:
                sys.exit('cannot use "-i -" with multiple inputs')
            input_paths = sys.stdin
        else:
            input_paths.extend(args.input)

    if args.file_of_paths:
        input_paths = itertools.chain(input_paths, pathfile_iter(args.file_of_paths))

    scp_config = config['streamcorpus_pipeline']
    stages = PipelineStages()
    if 'external_stages_path' in scp_config:
        stages.load_external_stages(scp_config['external_stages_path'])
    if 'external_stages_modules' in scp_config:
        for mod in scp_config['external_stages_modules']:
            stages.load_module_stages(mod)
    factory = PipelineFactory(stages)
    pipeline = factory(scp_config)

    for i_str in input_paths:
        logger.info('input path %r', i_str)
        work_unit = SimpleWorkUnit(i_str.strip())
        work_unit.data['start_chunk_time'] = time.time()
        work_unit.data['start_count'] = args.skip
        pipeline._process_task(work_unit)

class SimpleWorkUnit(object):
    '''partially duck-typed coordinate.WorkUnit that wraps strings from
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

    def fail(self, exc=None):
        logger.critical('failing SimpleWorkUnit(%r) = %r: %r', self.key, self.data, exc, exc_info=True)
        sys.exit(-1)


if __name__ == '__main__':
    main()
