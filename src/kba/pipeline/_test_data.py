
import os
from streamcorpus import Chunk, StreamItem_v0_2_0

def get_test_chunk_path():
    path = os.path.dirname(__file__)
    path = os.path.join( path, '../../../data/test/', 'WEBLOG-100-fd5f05c8a680faa2bf8c55413e949bbf.sc' )
    return path

def get_test_chunk():
    return Chunk(path=get_test_chunk_path(), message=StreamItem_v0_2_0)

def get_serif_test_path():

    path = os.path.dirname(__file__)
    # path = os.path.join( path, '../../../data/test/', 'smith-harris-normalized-serif-10.ent' )
    path = os.path.join( path, '../../../data/test/', 'smith-harris-normalized.ent' )
    return path
