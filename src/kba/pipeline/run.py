#!/usr/bin/python
'''
Operates a pipeline based on a configuration file
'''
import os
import sys
from . import Pipeline

if __name__ == '__main__':
    import yaml
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('config', metavar='config.yaml', 
                        help='configuration parameters for a pipeline run')
    args = parser.parse_args()

    assert os.path.exists(args.config), '%s does not exist' % args.config
    config = yaml.load(open(args.config))

    pipeline = Pipeline(config)
    pipeline.run(chunk_paths=sys.stdin)

