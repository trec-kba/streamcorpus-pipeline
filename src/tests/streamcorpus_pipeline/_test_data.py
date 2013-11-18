
import os
from streamcorpus import Chunk, StreamItem_v0_2_0


_TEST_DATA_ROOT = '../../../data'


def get_test_chunk_path():
    path = os.path.dirname(__file__)
    path = os.path.join( path, _TEST_DATA_ROOT, 'test', 'WEBLOG-100-fd5f05c8a680faa2bf8c55413e949bbf.sc' )
    return path

def get_test_chunk():
    return Chunk(path=get_test_chunk_path(), message=StreamItem_v0_2_0)

def get_test_v0_3_0_chunk_path():
    path = os.path.dirname(__file__)
    path = os.path.join( path, _TEST_DATA_ROOT, 'test', 'WEBLOG-100-fd5f05c8a680faa2bf8c55413e949bbf-v0_3_0.sc.xz' )
    return path

def get_test_v0_3_0_chunk_tagged_by_serif_path():
    path = os.path.dirname(__file__)
    path = os.path.join( path, _TEST_DATA_ROOT, 'test', 'WEBLOG-100-fd5f05c8a680faa2bf8c55413e949bbf-v0_3_0-tagged-by-serif.sc.xz' )
    return path

def get_john_smith_tagged_by_lingpipe_path():
    return os.path.join(
        os.path.dirname(__file__),
        _TEST_DATA_ROOT,
        'john-smith/john-smith-tagged-by-lingpipe-0.sc')
