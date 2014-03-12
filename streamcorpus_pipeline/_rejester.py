'''
This software is released under an MIT/X11 open source license.

Copyright 2012-2014 Diffeo, Inc.
'''
from __future__ import absolute_import
import argparse
import sys
import time

import json

import kvlayer
import streamcorpus_pipeline
from streamcorpus_pipeline.run import instantiate_config
from streamcorpus_pipeline._pipeline import PipelineFactory
from streamcorpus_pipeline._exceptions import ConfigurationError
from streamcorpus_pipeline.stages import PipelineStages
import yakonfig

def rejester_run_function(work_unit):
    with yakonfig.defaulted_config([kvlayer, streamcorpus_pipeline],
                                   config=work_unit.spec.get('config', {})):
        scp_config = yakonfig.get_global_config('streamcorpus_pipeline')
        stages = PipelineStages()
        if 'external_stages_path' in scp_config:
            stages.load_external_stages(scp_config['external_stages_path'])
        factory = PipelineFactory(stages)
        pipeline = factory(scp_config)

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

