#!/usr/bin/python
'''
See help(Pipeline) for details on configuring a Pipeline.

This software is released under an MIT/X11 open source license.

Copyright 2012 Diffeo, Inc.
'''
import os
import sys
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

    pipeline = Pipeline(config)
    pipeline.run()

