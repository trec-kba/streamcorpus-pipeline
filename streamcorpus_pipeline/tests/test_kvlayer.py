from __future__ import absolute_import
import contextlib
import logging
import uuid

import pytest

import kvlayer
import streamcorpus
from streamcorpus_pipeline._kvlayer import from_kvlayer, to_kvlayer
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
            'storage_type': 'redis',
            'storage_addresses': [redis_address],
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
    si = streamcorpus.make_stream_item('2000-01-01T12:34:00.000123Z',
                                       'test://test.stream.item/')
    chunkfile = str(tmpdir.join('chunk.sc.xz'))
    with streamcorpus.Chunk(path=chunkfile, mode='wb') as chunk:
        chunk.add(si)

    with configurator():
        writer = to_kvlayer(yakonfig.get_global_config(
            'streamcorpus_pipeline', 'to_kvlayer'))
        writer(chunkfile, {}, '')

        kvlclient = kvlayer.client()
        kvlclient.setup_namespace({'stream_items': 2})
        print repr(list(kvlclient.scan_keys('stream_items')))
        for (k,v) in kvlclient.get(
                'stream_items',
                (uuid.UUID(int=946730040),
                 uuid.UUID(hex='985c1e3ed73256cd9a399919fe93cf76'))):
            assert v is not None

        reader = from_kvlayer(yakonfig.get_global_config(
            'streamcorpus_pipeline', 'from_kvlayer'))
        sis = list(reader(''))
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
        writer(chunkfile, {}, '')

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
        client.setup_namespace({'stream_items': 2,
                                'stream_items_doc_id_epoch_ticks': 2,
                                'stream_items_with_source': 2})
        yield path, client

@pytest.mark.slow
def test_kvlayer_reader_and_writer(configurator, test_data_dir):
    with chunks(configurator, test_data_dir) as (path, client):
        ## check that index table was created
        all_doc_ids = set()
        all_epoch_ticks = set()
        for (doc_id, epoch_ticks), empty_data in client.scan('stream_items_doc_id_epoch_ticks'):
            all_doc_ids.add(doc_id)
            all_epoch_ticks.add(epoch_ticks)
        all_doc_ids = sorted(all_doc_ids)
        all_epoch_ticks = sorted(all_epoch_ticks)
        logger.info('%d doc_ids', len(all_doc_ids))

        ## make an reader
        config = yakonfig.get_global_config('streamcorpus_pipeline',
                                            'from_kvlayer')
        reader = from_kvlayer(config)

        ## test it with different i_str inputs:
        for i_str in ['', '0,,%d,' % 10**10, '%d,%s,%d,%s' %
                      (all_epoch_ticks[0],  all_doc_ids[0],
                       all_epoch_ticks[-1], all_doc_ids[-1]) ]:
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
        # We should not have written the doc_id_epoch_ticks index at all
        for k,v in client.scan('stream_items_doc_id_epoch_ticks'):
            assert False, 'epoch_ticks present! k={!r}'.format(k)
        # Every item in the ...with_source index should match a real item
        for k,v in client.scan('stream_items_with_source'):
            assert v == 'WEBLOG' # by inspection
            for kk,sixz in client.get('stream_items', k):
                errs,sibytes = streamcorpus.decrypt_and_uncompress(sixz)
                assert errs == []
                for si in streamcorpus.Chunk(data=sibytes):
                    assert si.source == v

@pytest.mark.slow
def test_kvlayer_both_indexes(configurator, test_data_dir):
    overlay = {
        'streamcorpus_pipeline': {
            'to_kvlayer': {
                'indexes': [ 'with_source', 'doc_id_epoch_ticks' ],
            },
        },
    }
    with chunks(configurator, test_data_dir, overlay) as (path, client):
        for k,v in client.scan('stream_items_doc_id_epoch_ticks'):
            assert v == r''
            (k2,k1) = k
            for kk,sixz in client.get('stream_items', (k1,k2)):
                pass
        for k,v in client.scan('stream_items_with_source'):
            for kk,sixz in client.get('stream_items', k):
                pass
