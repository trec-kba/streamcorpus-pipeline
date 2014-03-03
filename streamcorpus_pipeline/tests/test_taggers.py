from __future__ import absolute_import
import pytest
import tempfile
from streamcorpus import Chunk

import streamcorpus_pipeline
from streamcorpus_pipeline.stages import _init_stage
from streamcorpus_pipeline.tests._test_data import get_john_smith_tagged_by_lingpipe_without_labels_data
import yakonfig

@pytest.mark.parametrize("tagger,chain_selector", [
    ('name_align_labels', 'ALL'),
    pytest.mark.skipif('True', ('line_offset_align_labels', 'ALL')),
    ('byte_offset_align_labels', 'ALL'),
    ('name_align_labels', 'ANY_MULTI_TOKEN'),
])
def test_tagger_transform(tagger, chain_selector):
    config = {
        'streamcorpus_pipeline': {
            tagger: {
                'tagger_id': 'lingpipe',
                'annotator_id': 'bagga-and-baldwin',
                'chain_selector': chain_selector
            }
        }
    }
    with yakonfig.defaulted_config([streamcorpus_pipeline],
                                   config=config, validate=False):
        transform = _init_stage(tagger)
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
