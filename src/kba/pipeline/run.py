#!/usr/bin/python
'''
See help(Pipeline) for details on configuring a Pipeline.

This software is released under an MIT/X11 open source license.

Copyright 2012 Diffeo, Inc.
'''
import os
import sys
from . import Pipeline

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

    pipeline = Pipeline(config)
    pipeline.run()

