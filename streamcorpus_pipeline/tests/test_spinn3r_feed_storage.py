"""Unit tests for streamcorpus_pipeline._spinn3r_feed_storage."""

import os
from StringIO import StringIO
import urlparse

from google.protobuf.message import DecodeError
import pytest

from streamcorpus import Chunk
import streamcorpus_pipeline
from streamcorpus_pipeline._exceptions import ConfigurationError
from streamcorpus_pipeline._pipeline import Pipeline, PipelineFactory
from streamcorpus_pipeline.run import instantiate_config, SimpleWorkUnit
from streamcorpus_pipeline._spinn3r_feed_storage import \
    from_spinn3r_feed, ProtoStreamReader
from streamcorpus_pipeline.stages import PipelineStages
import yakonfig


@pytest.fixture
def filename(test_data_dir):
    """path for the spinn3r sample data file"""
    return os.path.join(test_data_dir, 'test/spinn3r.bin')


@pytest.fixture
def urls():
    """ordered list of item URLs in the spinn3r sample data file"""
    return [
        'http://www.generationbass.com/2014/01/27/'
        'the-great-dj-terrivel-tarraxinha/',
        'http://fontstruct.com/fontstructions/show/939228',
        'http://fontstruct.com/fontstructions/show/939218',
        'http://www.kfgo.com/news/articles/2014/jan/27/'
        'pennsylvania-lawmaker-accused-of-ghost-employee-scam/',
        'http://www.kfgo.com/news/articles/2014/jan/27/'
        'fabulist-glass-may-not-practice-law-in-california-court/',
        'http://www.kfgo.com/news/articles/2014/jan/27/'
        'jurors-being-chosen-in-delaware-doctors-trial-over-'
        'waterboarding-claims/',
        'http://www.kfgo.com/news/articles/2014/jan/27/'
        'fargo-will-not-prosecute-tv-reporter-for-secretly-entering-school/',
        'http://www.kfgo.com/news/articles/2014/jan/27/'
        'governor-daytons-bonding-proposal-devoid-of-funding-for-'
        'moorhead-flood-mitigation/',
        'http://www.kfgo.com/news/articles/2014/jan/27/'
        'minn-touts-new-indoor-stadium-for-2018-super-bowl/'
    ]


@pytest.fixture(scope='function')
def pipeline_config(tmpdir):
    """reference streamcorpus_pipeline configuration

    since this is scope='function', it is safe to modify this within a test,
    it will not affect other tests later on

    """
    return {
        'streamcorpus_pipeline': {
            'root_path': str(tmpdir),
            'log_level': 'INFO',
            'output_chunk_max_count': 500,
            'tmp_dir_path': 'tmp',
            'reader': 'from_spinn3r_feed',
            'from_spinn3r_feed': {},
            'incremental_transforms': ['language', 'guess_media_type'],
            'batch_transforms': [],
            'writers': ['to_local_chunks'],
            'to_local_chunks': {
                'output_type': 'otherdir',
                'output_path': str(tmpdir),
                'output_name': 'spinn3r'
            },
        }
    }


@pytest.fixture
def output_file(tmpdir):
    """the output file from pipeline_config"""
    return str(tmpdir.join('spinn3r.sc'))


def test_proto_stream_reader(filename, urls):
    with open(filename, 'rb') as f:
        reader = ProtoStreamReader(f)
        assert ([entry.source.canonical_link.href for entry in reader] == urls)


def test_proto_stream_reader_stringio(filename, urls):
    with open(filename, 'rb') as f:
        content = f.read()
    reader = ProtoStreamReader(StringIO(content))
    assert ([entry.source.canonical_link.href for entry in reader] == urls)


def test_feed_direct(filename, urls):
    """test from_spinn3r_feed directly feeding in the input"""
    feed3r = from_spinn3r_feed(config={})
    it = feed3r(filename)
    assert [si.abs_url for si in it] == urls


def test_spinn3r_pipeline(filename, urls, pipeline_config, output_file):
    """minimal end-to-end test, with a fixed pipeline"""
    with yakonfig.defaulted_config([streamcorpus_pipeline],
                                   config=pipeline_config):
        stages = PipelineStages()
        factory = PipelineFactory(stages)
        pipeline = factory(yakonfig.get_global_config('streamcorpus_pipeline'))
        work_unit = SimpleWorkUnit(filename)
        work_unit.data['start_chunk_time'] = 0
        work_unit.data['start_count'] = 0
        pipeline._process_task(work_unit)

        with Chunk(path=output_file, mode='rb') as chunk:
            assert [si.abs_url for si in chunk] == urls


def test_spinn3r_pipeline_unprefetched(urls, pipeline_config):
    """minimal end-to-end test, missing prefetched data"""
    pipeline_config['streamcorpus_pipeline']['from_spinn3r_feed'] = {
        'use_prefetched': True
    }
    with yakonfig.defaulted_config([streamcorpus_pipeline],
                                   config=pipeline_config):
        stages = PipelineStages()
        factory = PipelineFactory(stages)
        pipeline = factory(yakonfig.get_global_config('streamcorpus_pipeline'))
        key = 'test_file.bin'
        work_unit = SimpleWorkUnit(key)
        work_unit.data['start_chunk_time'] = 0
        work_unit.data['start_count'] = 0
        with pytest.raises(ConfigurationError):
            pipeline._process_task(work_unit)


def test_spinn3r_pipeline_prefetched(filename, urls, pipeline_config,
                                     output_file):
    """minimal end-to-end test, preloading data in the loader"""
    pipeline_config['streamcorpus_pipeline']['from_spinn3r_feed'] = {
        'use_prefetched': True
    }
    with yakonfig.defaulted_config([streamcorpus_pipeline],
                                   config=pipeline_config):
        stages = PipelineStages()
        factory = PipelineFactory(stages)
        pipeline = factory(yakonfig.get_global_config('streamcorpus_pipeline'))
        assert isinstance(pipeline.reader, from_spinn3r_feed)
        key = 'test_file.bin'
        with open(filename, 'rb') as f:
            pipeline.reader.prefetched[key] = f.read()
        work_unit = SimpleWorkUnit(key)
        work_unit.data['start_chunk_time'] = 0
        work_unit.data['start_count'] = 0
        pipeline._process_task(work_unit)

        with Chunk(path=output_file, mode='rb') as chunk:
            assert [si.abs_url for si in chunk] == urls


def test_spinn3r_pipeline_bogus_prefetched(filename, pipeline_config):
    """supply known-bad prefetched data"""
    with yakonfig.defaulted_config([streamcorpus_pipeline],
                                   config=pipeline_config):
        stages = PipelineStages()
        factory = PipelineFactory(stages)
        pipeline = factory(yakonfig.get_global_config('streamcorpus_pipeline'))
        assert isinstance(pipeline.reader, from_spinn3r_feed)
        key = filename
        pipeline.reader.prefetched[key] = 'bogus data, dude!'
        work_unit = SimpleWorkUnit(key)
        work_unit.data['start_chunk_time'] = 0
        work_unit.data['start_count'] = 0
        with pytest.raises(DecodeError):
            pipeline._process_task(work_unit)


def test_spinn3r_pipeline_ignore_prefetched(filename, urls, pipeline_config,
                                            output_file):
    """configuration explicitly ignores bad prefetched data"""
    pipeline_config['streamcorpus_pipeline']['from_spinn3r_feed'] = {
        'use_prefetched': False
    }
    with yakonfig.defaulted_config([streamcorpus_pipeline],
                                   config=pipeline_config):
        stages = PipelineStages()
        factory = PipelineFactory(stages)
        pipeline = factory(yakonfig.get_global_config('streamcorpus_pipeline'))
        assert isinstance(pipeline.reader, from_spinn3r_feed)
        key = filename
        pipeline.reader.prefetched[key] = 'bogus data, dude!'
        work_unit = SimpleWorkUnit(key)
        work_unit.data['start_chunk_time'] = 0
        work_unit.data['start_count'] = 0
        pipeline._process_task(work_unit)

        with Chunk(path=output_file, mode='rb') as chunk:
            assert [si.abs_url for si in chunk] == urls


def test_spinn3r_pipeline_filter_matches(filename, urls, pipeline_config,
                                         output_file):
    """set a publisher_type filter that matches everything in the feed"""
    pipeline_config['streamcorpus_pipeline']['from_spinn3r_feed'] = {
        'publisher_type': 'WEBLOG'
    }
    with yakonfig.defaulted_config([streamcorpus_pipeline],
                                   config=pipeline_config):
        stages = PipelineStages()
        factory = PipelineFactory(stages)
        pipeline = factory(yakonfig.get_global_config('streamcorpus_pipeline'))
        work_unit = SimpleWorkUnit(filename)
        work_unit.data['start_chunk_time'] = 0
        work_unit.data['start_count'] = 0
        pipeline._process_task(work_unit)

        with Chunk(path=output_file, mode='rb') as chunk:
            assert [si.abs_url for si in chunk] == urls


def test_spinn3r_pipeline_filter_no_matches(filename, pipeline_config,
                                            output_file):
    """set a publisher_type filter that matches nothing in the feed"""
    pipeline_config['streamcorpus_pipeline']['from_spinn3r_feed'] = {
        'publisher_type': 'MICROBLOG'
    }
    with yakonfig.defaulted_config([streamcorpus_pipeline],
                                   config=pipeline_config):
        stages = PipelineStages()
        factory = PipelineFactory(stages)
        pipeline = factory(yakonfig.get_global_config('streamcorpus_pipeline'))
        work_unit = SimpleWorkUnit(filename)
        work_unit.data['start_chunk_time'] = 0
        work_unit.data['start_count'] = 0
        pipeline._process_task(work_unit)

        # no chunks means the output file won't actually get written
        assert not os.path.exists(output_file)
