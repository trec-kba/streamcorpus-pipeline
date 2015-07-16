from __future__ import absolute_import
import logging
import time

import pytest

from streamcorpus import make_stream_item
import streamcorpus_pipeline
import streamcorpus_pipeline.config
from streamcorpus_pipeline.stages import PipelineStages
from streamcorpus_pipeline._local_storage import to_local_chunks
from streamcorpus_pipeline._pipeline import PipelineFactory, Pipeline
from streamcorpus_pipeline.run import SimpleWorkUnit
from streamcorpus_pipeline.tests._test_data import get_test_chunk_path
import yakonfig

logger = logging.getLogger(__name__)


@pytest.mark.slow
def test_pipeline(request, test_data_dir):
    filename = str(request.fspath.dirpath('test_dedup_chunk_counts.yaml'))
    with yakonfig.defaulted_config([streamcorpus_pipeline],
                                   filename=filename):
        # run the pipeline
        stages = PipelineStages()
        pf = PipelineFactory(stages)
        p = pf(yakonfig.get_global_config('streamcorpus_pipeline'))

        work_unit = SimpleWorkUnit(get_test_chunk_path(test_data_dir))
        work_unit.data['start_chunk_time'] = time.time()
        work_unit.data['start_count'] = 0
        p._process_task(work_unit)


@pytest.mark.slow
def test_post_batch_incremental_stage(request, test_data_dir):
    filename = str(request.fspath.dirpath('test_post_batch_incremental.yaml'))
    with yakonfig.defaulted_config([streamcorpus_pipeline],
                                   filename=filename):
        stages = PipelineStages()
        pf = PipelineFactory(stages)
        p = pf(yakonfig.get_global_config('streamcorpus_pipeline'))
        p.run(get_test_chunk_path(test_data_dir))


class ExternalStage(streamcorpus_pipeline.stages.Configured):
    config_name = 'external_stage'
    default_config = {'message': 'default message'}

    def get_message(self):
        return self.config.get('message', None)

Stages = {'external_stage': ExternalStage}


def test_external_stage_unregistered(tmpdir):
    streamcorpus_pipeline.config.static_stages = None
    with yakonfig.defaulted_config([streamcorpus_pipeline], config={
            'streamcorpus_pipeline': {
                'tmp_dir_path': str(tmpdir),
                'external_stage': {
                    'message': 'configured message',
                },
            },
    }):
        config = yakonfig.get_global_config('streamcorpus_pipeline',
                                            'external_stage')
        stage = ExternalStage(config=config)
        assert stage.get_message() == 'configured message'


def test_external_stage_registered(tmpdir):
    streamcorpus_pipeline.config.static_stages = None
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
        config = yakonfig.get_global_config('streamcorpus_pipeline',
                                            'external_stage')
        stage = ExternalStage(config=config)
        assert stage.get_message() == 'configured message'


def test_external_stage_default(tmpdir):
    streamcorpus_pipeline.config.static_stages = None
    with yakonfig.defaulted_config([streamcorpus_pipeline], config={
            'streamcorpus_pipeline': {
                'external_stages_path': __file__,
                'reader': 'from_local_chunks',
                'writers': ['to_local_chunks'],
                'tmp_dir_path': str(tmpdir),
            },
    }):
        config = yakonfig.get_global_config('streamcorpus_pipeline',
                                            'external_stage')
        stage = ExternalStage(config=config)
        assert stage.get_message() == 'default message'


def test_external_module_registered(tmpdir):
    streamcorpus_pipeline.config.static_stages = None
    with yakonfig.defaulted_config([streamcorpus_pipeline], config={
            'streamcorpus_pipeline': {
                'external_stages_modules':
                ['streamcorpus_pipeline.tests.test_pipeline'],
                'external_stage': {
                    'message': 'configured message',
                },
                'reader': 'from_local_chunks',
                'writers': ['to_local_chunks'],
                'tmp_dir_path': str(tmpdir),
            },
    }):
        config = yakonfig.get_global_config('streamcorpus_pipeline',
                                            'external_stage')
        stage = ExternalStage(config=config)
        assert stage.get_message() == 'configured message'


def test_external_module_default(tmpdir):
    streamcorpus_pipeline.config.static_stages = None
    with yakonfig.defaulted_config([streamcorpus_pipeline], config={
            'streamcorpus_pipeline': {
                'external_stages_modules':
                ['streamcorpus_pipeline.tests.test_pipeline'],
                'reader': 'from_local_chunks',
                'writers': ['to_local_chunks'],
                'tmp_dir_path': str(tmpdir),
            },
    }):
        config = yakonfig.get_global_config('streamcorpus_pipeline',
                                            'external_stage')
        stage = ExternalStage(config=config)
        assert stage.get_message() == 'default message'


class TwoItemReader():
    def __init__(self):
        pass

    def __call__(self, i_str):
        si = make_stream_item(time.time(), 'file:///tmp/si1.sc')
        yield si
        si = make_stream_item(time.time(), 'file:///tmp/si2.sc')
        yield si


def test_one_si_per_output(tmpdir):
    '''with output_chunk_max_count=1, write one chunk file per SI'''
    reader = TwoItemReader()
    writer = to_local_chunks({'output_type': 'otherdir',
                              'output_name': 'output-%(first)d-%(num)d',
                              'output_path': str(tmpdir),
                              'cleanup_tmp_files': True})
    p = Pipeline(1000, 1000, False, str(tmpdir), True,
                 1,  # output_chunk_max_count -- this is key
                 None, reader, [], [], [], [writer])
    wu = SimpleWorkUnit('input')
    wu.data['start_count'] = 0
    wu.data['start_chunk_time'] = 0
    p._process_task(wu)

    assert tmpdir.join('output-0-1.sc').check()
    assert tmpdir.join('output-1-1.sc').check()
    assert wu.data['output'] == [str(tmpdir.join('output-0-1.sc')),
                                 str(tmpdir.join('output-1-1.sc'))]
