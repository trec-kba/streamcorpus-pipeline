
import os
from streamcorpus import Chunk, StreamItem_v0_2_0, add_annotation
from StringIO import StringIO


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

def get_john_smith_tagged_by_lingpipe_without_labels_data():
    fh = StringIO()
    o_chunk = Chunk(file_obj=fh, mode='wb')

    path = get_john_smith_tagged_by_lingpipe_path()
    for si in Chunk(path):
        for sentence in si.body.sentences['lingpipe']:
            for token in sentence.tokens:
                for labels in token.labels.values():
                    for label in labels:
                        label.offsets.update(token.offsets)
                        for offset in label.offsets.values():
                            offset.value = token.token
                        add_annotation(si.body, label)
                token.labels = dict()
        o_chunk.add(si)

    o_chunk.flush()
    return fh.getvalue()
