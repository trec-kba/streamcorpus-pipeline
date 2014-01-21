
import time
import errno
import pytest
import kvlayer
import streamcorpus
from _test_data import get_test_v0_3_0_chunk_path
from streamcorpus_pipeline._kvlayer import from_kvlayer, to_kvlayer
from streamcorpus_pipeline._logging import logger

@pytest.fixture(scope='function')
def config(request):
    config = dict(
        namespace='tests',
        app_name='streamcorpus_pipeline',
        storage_type='cassandra',
        storage_addresses=['test-cassandra-1.diffeo.com:9160'],
        connection_pool_size=2,
        max_consistency_delay=120,
        replication_factor=1,
        thrift_framed_transport_size_in_mb=15,
    )
    def fin():
        client = kvlayer.client(config)
        client.delete_namespace()
    request.addfinalizer(fin)
    return config

def test_kvlayer_reader_and_writer(config):
    path = get_test_v0_3_0_chunk_path()
    writer = to_kvlayer(config)
    
    ## name_info and i_str are not used by the writer
    i_str = ''
    name_info = {}
    writer(path, name_info, i_str)

    ## check that index table was created
    all_doc_ids = set()
    all_epoch_ticks = set()
    for (doc_id, epoch_ticks), empty_data in writer.client.scan('stream_items_doc_id_epoch_ticks'):
        all_doc_ids.add(doc_id)
        all_epoch_ticks.add(epoch_ticks)
    all_doc_ids = sorted(all_doc_ids)
    all_epoch_ticks = sorted(all_epoch_ticks)
    logger.info('%d doc_ids', len(all_doc_ids))

    ## make an reader
    reader = from_kvlayer(config)

    ## test it with different i_str inputs:
    for i_str in ['', '0,,%d,' % 10**10, '%d,%s,%d,%s' % (all_epoch_ticks[0],  all_doc_ids[0],
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

