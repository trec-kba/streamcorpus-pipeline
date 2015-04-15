
import os
from streamcorpus import Chunk, StreamItem_v0_2_0, add_annotation
from StringIO import StringIO


def get_si_irish_duo_tagged_by_basis(test_data_dir):
    path = os.path.join(test_data_dir, 'test',
                        'irish-duo-tagged-by-basis.sc')
    chunk = Chunk(path=path)
    return next(iter(chunk))


def get_si_random_chinese_tagged_by_basis(test_data_dir):
    path = os.path.join(test_data_dir, 'test',
                        'random-chinese-tagged-by-basis.sc')
    chunk = Chunk(path=path)
    return next(iter(chunk))


def get_si_wikipedia_chinese_tagged_by_basis(test_data_dir):
    path = os.path.join(test_data_dir, 'test',
                        'wikipedia-chinese-tagged-by-basis.sc')
    chunk = Chunk(path=path)
    return next(iter(chunk))


def get_si_simple_tagged_by_basis(test_data_dir):
    path = os.path.join(test_data_dir, 'test', 'simple-tagged-by-basis.sc')
    chunk = Chunk(path=path)
    return next(iter(chunk))


def get_test_chunk_path(test_data_dir):
    path = os.path.join(test_data_dir, 'test', 'WEBLOG-100-fd5f05c8a680faa2bf8c55413e949bbf.sc.xz' )
    return path

def get_test_chunk(test_data_dir):
    return Chunk(path=get_test_chunk_path(test_data_dir), message=StreamItem_v0_2_0)

def get_test_v0_3_0_chunk_path(test_data_dir):
    path = os.path.join(test_data_dir, 'test', 'WEBLOG-100-fd5f05c8a680faa2bf8c55413e949bbf-v0_3_0.sc.xz' )
    return path

def get_test_v0_3_0_chunk_tagged_by_serif_path(test_data_dir):
    path = os.path.join(test_data_dir, 'test', 'WEBLOG-100-fd5f05c8a680faa2bf8c55413e949bbf-v0_3_0-tagged-by-serif.sc.xz')
    return path

def get_john_smith_tagged_by_lingpipe_path(test_data_dir):
    return os.path.join(
        test_data_dir,
        'john-smith/john-smith-tagged-by-lingpipe-serif-0-197.sc.xz')

def get_john_smith_tagged_by_lingpipe_without_labels_data(test_data_dir):
    fh = StringIO()
    o_chunk = Chunk(file_obj=fh, mode='wb')

    path = get_john_smith_tagged_by_lingpipe_path(test_data_dir)
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
