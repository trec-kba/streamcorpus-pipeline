
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

def test_kvlayer_extractor_and_loader(config):
    path = get_test_v0_3_0_chunk_path()
    loader = to_kvlayer(config)
    loader(path, {}, '')
    extractor = from_kvlayer(config)
    stream_ids = []
    for si in extractor( '0,,%d,' % 10**10 ):
        stream_ids.append(si.stream_id)    
    _input_chunk_ids = [si.stream_id for si in streamcorpus.Chunk(path)]
    input_chunk_ids = list(set(_input_chunk_ids))
    logger.info('%d inserts, %d unique',
                len(_input_chunk_ids), len(input_chunk_ids))
    input_chunk_ids.sort()
    stream_ids.sort()
    assert len(input_chunk_ids) == len(stream_ids)
    assert input_chunk_ids == stream_ids
