from __future__ import absolute_import
from cStringIO import StringIO
import logging
import os
import signal
import sys
import time

import gevent
import pytest
import yaml

import streamcorpus_pipeline
from streamcorpus_pipeline.stages import PipelineStages
from streamcorpus_pipeline._pipeline import PipelineFactory
from streamcorpus_pipeline.tests._test_data import get_test_chunk_path, \
    get_test_chunk, \
    get_test_v0_3_0_chunk_path, \
    get_test_v0_3_0_chunk_tagged_by_serif_path
import yakonfig

logger = logging.getLogger(__name__)

class SuccessfulExit(Exception):
    pass

def test_pipeline(request, test_data_dir):
    filename=str(request.fspath.dirpath('test_dedup_chunk_counts.yaml'))
    with yakonfig.defaulted_config([streamcorpus_pipeline], filename=filename):
        ## config says read from stdin, so make that have what we want
        stdin = sys.stdin
        sys.stdin = StringIO(get_test_chunk_path(test_data_dir))

        ## run the pipeline
        stages = PipelineStages()
        pf = PipelineFactory(stages)
        p = pf(yakonfig.get_global_config('streamcorpus_pipeline'))

        from streamcorpus_pipeline.run import SimpleWorkUnit
        work_unit = SimpleWorkUnit('long string indicating source of text')
        work_unit.data['start_chunk_time'] = time.time()
        work_unit.data['start_count'] = 0
        g = gevent.spawn(p._process_task, work_unit)

        gevent.sleep(5)

        logger.debug('now joining...')
        timeout = gevent.Timeout(1)
        g.join(timeout=timeout)


@pytest.mark.skipif('True')  # pylint: disable=E1101
def test_post_batch_incremental_stage(request, test_data_dir):
    path = os.path.dirname(__file__)
    config = yaml.load(open(os.path.join(path, 'test_post_batch_incremental.yaml')))

    ## config says read from stdin, so make that have what we want
    stdin = sys.stdin
    sys.stdin = StringIO(get_test_chunk_path(test_data_dir))

    ## run the pipeline
    p = Pipeline( config )
    p.run()

@pytest.mark.skipif('True')  # pylint: disable=E1101
def test_align_serif_stage(test_data_dir):
    path = os.path.dirname(__file__)
    config = yaml.load(open(os.path.join(path, 'test_align_serif_stage.yaml')))

    ## config says read from stdin, so make that have what we want
    stdin = sys.stdin
    sys.stdin = StringIO(get_test_v0_3_0_chunk_tagged_by_serif_path(test_data_dir))

    ## run the pipeline
    p = Pipeline( config )
    p.run()

class ExternalStage(streamcorpus_pipeline.stages.Configured):
    config_name = 'external_stage'
    default_config = { 'message': 'default message' }
    def get_message(self):
        return self.config.get('message', None)
Stages = { 'external_stage': ExternalStage }

def test_external_stage_unregistered(tmpdir):
    with yakonfig.defaulted_config([streamcorpus_pipeline], config={
            'streamcorpus_pipeline': {
                'tmp_dir_path': str(tmpdir),
                'external_stage': {
                    'message': 'configured message',
                },
            },
    }):
        config=yakonfig.get_global_config('streamcorpus_pipeline',
                                          'external_stage')
        stage = ExternalStage(config=config)
        assert stage.get_message() == 'configured message'

def test_external_stage_registered(tmpdir):
    with yakonfig.defaulted_config([streamcorpus_pipeline], config={
            'streamcorpus_pipeline': {
                'external_stages_path': __file__,
                'external_stage': {
                    'message': 'configured message',
                },
                'reader': 'from_local_chunks',
                'writers': ['to_local_chunks'],
                'tmp_dir_path': str(tmpdir),
            },
    }):
        config=yakonfig.get_global_config('streamcorpus_pipeline',
                                          'external_stage')
        stage = ExternalStage(config=config)
        assert stage.get_message() == 'configured message'

def test_external_stage_default(tmpdir):
    with yakonfig.defaulted_config([streamcorpus_pipeline], config={
            'streamcorpus_pipeline': {
                'external_stages_path': __file__,
                'reader': 'from_local_chunks',
                'writers': ['to_local_chunks'],
                'tmp_dir_path': str(tmpdir),
            },
    }):
        config=yakonfig.get_global_config('streamcorpus_pipeline',
                                          'external_stage')
        stage = ExternalStage(config=config)
        assert stage.get_message() == 'default message'

def test_external_module_registered(tmpdir):
    with yakonfig.defaulted_config([streamcorpus_pipeline], config={
            'streamcorpus_pipeline': {
                'external_stages_modules':
                [ 'streamcorpus_pipeline.tests.test_pipeline' ],
                'external_stage': {
                    'message': 'configured message',
                },
                'reader': 'from_local_chunks',
                'writers': ['to_local_chunks'],
                'tmp_dir_path': str(tmpdir),
            },
    }):
        config=yakonfig.get_global_config('streamcorpus_pipeline',
                                          'external_stage')
        stage = ExternalStage(config=config)
        assert stage.get_message() == 'configured message'

def test_external_module_default(tmpdir):
    with yakonfig.defaulted_config([streamcorpus_pipeline], config={
            'streamcorpus_pipeline': {
                'external_stages_modules':
                [ 'streamcorpus_pipeline.tests.test_pipeline' ],
                'reader': 'from_local_chunks',
                'writers': ['to_local_chunks'],
                'tmp_dir_path': str(tmpdir),
            },
    }):
        config=yakonfig.get_global_config('streamcorpus_pipeline',
                                          'external_stage')
        stage = ExternalStage(config=config)
        assert stage.get_message() == 'default message'
