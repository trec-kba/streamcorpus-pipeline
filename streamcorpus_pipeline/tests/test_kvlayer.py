'''tests for {to,from}_kvlayer reader/writer stages

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2014 Diffeo, Inc.
'''
from __future__ import absolute_import
from collections import defaultdict
import contextlib
import hashlib
import logging
import uuid

from backports import lzma
import kvlayer
import pytest
import streamcorpus

from streamcorpus_pipeline._kvlayer import from_kvlayer, to_kvlayer, serialize_si_key
from streamcorpus_pipeline._kvlayer_keyword_search import keyword_indexer, \
    INDEXING_DEPENDENCIES_FAILED
from streamcorpus_pipeline._kvlayer_table_names import STREAM_ITEMS_TABLE, HASH_KEYWORD, HASH_TF_SID, STREAM_ITEM_TABLE_DEFS, STREAM_ITEMS_SOURCE_INDEX
from streamcorpus_pipeline.tests._test_data import get_test_v0_3_0_chunk_path
import yakonfig

logger = logging.getLogger(__name__)

class MiniScp(object):
    config_name = 'streamcorpus_pipeline'
    sub_modules = [from_kvlayer, to_kvlayer]

@pytest.fixture(scope='function')
def configurator(request, namespace_string, redis_address):
    base_config = {
        'kvlayer': {
            'storage_type': 'local',
            #'storage_addresses': [redis_address],
            'app_name': 'streamcorpus_pipeline',
            'namespace': namespace_string,
        },
        'streamcorpus_pipeline': {
            'to_kvlayer': {},
            'from_kvlayer': {},
        },
    }
    @contextlib.contextmanager
    def make_config(overlay={}):
        config = yakonfig.merge.overlay_config(base_config, overlay)
        with yakonfig.defaulted_config([kvlayer, MiniScp], config=config):
            yield
            client = kvlayer.client()
            client.delete_namespace()
    return make_config

def test_kvlayer_simple(configurator, tmpdir):
    abs_url = 'test://test.stream.item/'
    si = streamcorpus.make_stream_item('2000-01-01T12:34:00.000123Z',
                                       abs_url)
    chunkfile = str(tmpdir.join('chunk.sc.xz'))
    with streamcorpus.Chunk(path=chunkfile, mode='wb') as chunk:
        chunk.add(si)

    with configurator():
        writer = to_kvlayer(yakonfig.get_global_config(
            'streamcorpus_pipeline', 'to_kvlayer'))
        stream_ids = writer(chunkfile, {}, '')
        stream_ids = list(stream_ids)
        assert len(stream_ids) == 1
        assert stream_ids[0] == '\x98\\\x1e>\xd72V\xcd\x9a9\x99\x19\xfe\x93\xcfv8m\xf48'

        kvlclient = kvlayer.client()
        kvlclient.setup_namespace(STREAM_ITEM_TABLE_DEFS)
        print repr(list(kvlclient.scan_keys(STREAM_ITEMS_TABLE)))
        for (k,v) in kvlclient.get(
                STREAM_ITEMS_TABLE,
                (hashlib.md5(abs_url).digest(), 946730040)):
            assert v is not None

        reader = from_kvlayer(yakonfig.get_global_config(
            'streamcorpus_pipeline', 'from_kvlayer'))
        sis = list(reader(''))
        assert len(sis) == 1
        assert sis[0].stream_time.epoch_ticks == si.stream_time.epoch_ticks
        assert sis[0].abs_url == si.abs_url

        # test streamid "{time}-{hash}" i_str format
        reader = from_kvlayer(yakonfig.get_global_config(
            'streamcorpus_pipeline', 'from_kvlayer'))
        sis = list(reader(stream_ids[0]))
        assert len(sis) == 1
        assert sis[0].stream_time.epoch_ticks == si.stream_time.epoch_ticks
        assert sis[0].abs_url == si.abs_url

def test_kvlayer_negative(configurator, tmpdir):
    si = streamcorpus.make_stream_item('1969-07-20T20:18:00.000000Z',
                                       'test://test.stream.item/')
    chunkfile = str(tmpdir.join('chunk.sc.xz'))
    with streamcorpus.Chunk(path=chunkfile, mode='wb') as chunk:
        chunk.add(si)

    with configurator():
        writer = to_kvlayer(yakonfig.get_global_config(
            'streamcorpus_pipeline', 'to_kvlayer'))
        stream_ids = writer(chunkfile, {}, '')
        for sid in stream_ids:
            pass # exercise the generator

        reader = from_kvlayer(yakonfig.get_global_config(
            'streamcorpus_pipeline', 'from_kvlayer'))
        sis = list(reader(''))
        assert len(sis) == 1
        assert sis[0].stream_time.epoch_ticks == si.stream_time.epoch_ticks
        assert sis[0].abs_url == si.abs_url

@contextlib.contextmanager
def chunks(configurator, test_data_dir, overlay={}):
    with configurator(overlay):
        path = get_test_v0_3_0_chunk_path(test_data_dir)
        config = yakonfig.get_global_config('streamcorpus_pipeline',
                                            'to_kvlayer')
        writer = to_kvlayer(config)

        ## name_info and i_str are not used by the writer
        i_str = ''
        name_info = {}
        writer(path, name_info, i_str)

        client = kvlayer.client()
        client.setup_namespace(STREAM_ITEM_TABLE_DEFS)
        yield path, client

@pytest.mark.slow
def test_kvlayer_reader_and_writer(configurator, test_data_dir):
    with chunks(configurator, test_data_dir) as (path, client):
        ## check that index table was created
        all_si_keys = []
        for si_key in client.scan_keys(STREAM_ITEMS_TABLE):
            all_si_keys.append(si_key)
        logger.info('%d doc_ids', len(all_si_keys))

        ## make an reader
        config = yakonfig.get_global_config('streamcorpus_pipeline',
                                            'from_kvlayer')
        reader = from_kvlayer(config)

        ## test it with different i_str inputs:
        all_keys_istr = serialize_si_key(all_si_keys[0]) + '<' + serialize_si_key(all_si_keys[-1])
        for i_str in [
                '',
                all_keys_istr]:
            stream_ids = []
            for si in reader(i_str):
                stream_ids.append(si.stream_id)
            _input_chunk_ids = [si.stream_id for si in streamcorpus.Chunk(path)]
            input_chunk_ids = list(set(_input_chunk_ids))
            logger.info('%d inserts, %d unique',
                        len(_input_chunk_ids), len(input_chunk_ids))
            input_chunk_ids.sort()
            stream_ids.sort()
            assert len(input_chunk_ids) == len(stream_ids)
            assert input_chunk_ids == stream_ids

@pytest.mark.slow
def test_kvlayer_index_with_source(configurator, test_data_dir):
    overlay = {
        'streamcorpus_pipeline': {
            'to_kvlayer': {
                'indexes': [ 'with_source' ],
            },
        },
    }
    with chunks(configurator, test_data_dir, overlay) as (path, client):
        # Every item in the ...with_source index should match a real item
        for k,v in client.scan(STREAM_ITEMS_SOURCE_INDEX):
            assert v == 'WEBLOG' # by inspection
            for kk,sixz in client.get(STREAM_ITEMS_TABLE, k):
                errs,sibytes = streamcorpus.decrypt_and_uncompress(sixz)
                assert errs == []
                for si in streamcorpus.Chunk(data=sibytes):
                    assert si.source == v

@pytest.mark.slow
def test_kvlayer_both_indexes(configurator, test_data_dir):
    overlay = {
        'streamcorpus_pipeline': {
            'to_kvlayer': {
                'indexes': [ 'with_source' ],
            },
        },
    }
    with chunks(configurator, test_data_dir, overlay) as (path, client):
        total = 0
        for k,v in client.scan(STREAM_ITEMS_SOURCE_INDEX):
            total += 1
            count = 0
            for kk,sixz in client.get(STREAM_ITEMS_TABLE, k):
                count += 1
            assert count == 1
        assert total == 31


@pytest.mark.slow
@pytest.mark.skipif(INDEXING_DEPENDENCIES_FAILED is not None,
                    reason=str(INDEXING_DEPENDENCIES_FAILED))
def test_kvlayer_keyword_indexes(configurator, test_data_dir):
    overlay = {
        'streamcorpus_pipeline': {
            'to_kvlayer': {
                'indexes': [ HASH_KEYWORD, HASH_TF_SID ],
            },
        },
    }
    truth_data = {
        'elephant': 0,
        'ipad': 1,
        'lunar': 1,
        'francais': 4,
        'auto': 2,
        'fachabteilungen': 1
        }
        
    with chunks(configurator, test_data_dir, overlay) as (path, client):

        indexer = keyword_indexer(client)

        ## check that our truth counts above match the corpus
        found = defaultdict(set)
        for _, xz_blob in client.scan(table_name()):
            thrift_blob = lzma.decompress(xz_blob)
            si = streamcorpus.deserialize(thrift_blob)
            tokens = set(indexer.analyzer(si.body.clean_visible))
            for keyword in truth_data:
                if keyword in tokens:
                    found[keyword].add(si)

        for keyword in found:
            found_count = len(found[keyword])
            assert found_count == truth_data[keyword], keyword

        ## verify that precision is perfect for this small data set
        for keyword, expected_count in truth_data.items():
            count = 0
            for si in indexer.search(keyword):
                count += 1
                tokens = set(indexer.analyzer(si.body.clean_visible))
                assert keyword in tokens
            assert count == expected_count
