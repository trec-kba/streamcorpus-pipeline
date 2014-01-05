from __future__ import absolute_import
import pytest
import tempfile
from streamcorpus import Chunk

from streamcorpus_pipeline._taggers import name_align_labels, line_offset_align_labels, byte_offset_align_labels
from tests.streamcorpus_pipeline._test_data import get_john_smith_tagged_by_lingpipe_without_labels_data


def _test_config():
    return {
        'tagger_id': 'lingpipe',
        'annotator_id': 'bagga-and-baldwin',
        'chain_selector': 'ALL',
    }


def test_name_align_labels():
    config = _test_config()
    transform = name_align_labels(config)
    _test_tagger_transform(transform)

@pytest.mark.skipif('True')
def test_line_offset_align_labels():
    config = _test_config()
    transform = line_offset_align_labels(config)
    _test_tagger_transform(transform)

def test_byte_offset_align_labels():
    config = _test_config()
    transform = byte_offset_align_labels(config)
    _test_tagger_transform(transform)


def _test_tagger_transform(transform):
    data = get_john_smith_tagged_by_lingpipe_without_labels_data()
    with tempfile.NamedTemporaryFile(suffix='.sc') as tf:
        tf.write(data)
        tf.flush()
        transform.process_path(tf.name)
        found_one = False
        for si in Chunk(tf.name):
            for sentence in si.body.sentences['lingpipe']:
                for token in sentence.tokens:
                    if token.labels:
                        found_one = True
        assert found_one

def test_tagger_any_multi_token_mentions():
    config = {
        'tagger_id': 'lingpipe',
        'annotator_id': 'bagga-and-baldwin',
        'chain_selector': 'ANY_MULTI_TOKEN',
    }
    transform = name_align_labels(config)
    _test_tagger_transform(transform)
