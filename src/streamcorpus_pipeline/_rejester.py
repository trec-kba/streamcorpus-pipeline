'''
This software is released under an MIT/X11 open source license.

Copyright 2012-2013 Diffeo, Inc.
'''
import sys
import json
import argparse
from streamcorpus_pipeline.run import instantiate_config
from streamcorpus_pipeline._pipeline import Pipeline
from streamcorpus_pipeline._exceptions import ConfigurationError


def rejester_run_function(work_unit):

    config = work_unit.spec.get('streamcorpus_pipeline')
    if not config:
        raise ConfigurationError('spec is missing streamcorpus_pipeline: %r' % work_unit.spec)

    instantiate_config(config)

    pipeline = Pipeline(config)

    work_unit.data['start_chunk_time'] = time.time()
    work_unit.data['start_count'] = 0
    pipeline._process_task(work_unit)


def rejester_terminate_function(work_unit):

    pass


def make_work_units():
    '''reads path strings over stdin and generates lines of JSON that
    can be loaded into rejester
    '''
    usage = 'cat list-of-paths | streamcorpus_pipeline_work_units | rejester load <namespace> -u - -w spec.yaml'
    parser = argparse.ArgumentParser(usage=usage)
    parser.parse_args()    

    for line in sys.stdin:
        line = line.strip()
        print json.dumps({line: dict(start_count=0, start_chunk_time=0)})

