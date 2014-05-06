from __future__ import absolute_import

import logging
import os
import uuid
import shutil

import pytest

import streamcorpus
import streamcorpus_pipeline
from streamcorpus import OffsetType
from streamcorpus_pipeline._lingpipe import lingpipe, only_whitespace
from streamcorpus_pipeline._taggers import byte_offset_align_labels
from streamcorpus_pipeline.tests.test_hyperlink_labels import \
    make_hyperlink_labeled_test_stream_item, \
    make_hyperlink_labeled_test_chunk
import yakonfig

logger = logging.getLogger(__name__)

def test_only_whitespace():
    assert only_whitespace.match('\n\t     \t    \n\t \n  ')
    assert only_whitespace.match('\n')
    assert only_whitespace.match('')
    assert only_whitespace.match(u'\u200b')
    assert only_whitespace.match(u'\n\n   \u200b')
    assert not only_whitespace.match(u'\u200b foo')
    assert not only_whitespace.match('\n\nh  ')

@pytest.mark.integration
def test_aligner(tmpdir, request, test_data_dir, third_dir):

    si = make_hyperlink_labeled_test_stream_item(test_data_dir)
    assert len(si.body.clean_visible) > 200
    #for x in si.body.labels['author']:
    #    print x.offsets[OffsetType.BYTES].first, x.offsets[OffsetType.BYTES].value, x.target.target_id
    c_path = str(tmpdir.join('chunk.sc'))
    chunk = streamcorpus.Chunk(c_path, mode='wb')
    chunk.add(si)
    chunk.close()

    lp = lingpipe(config={
        'tmp_dir_path': str(tmpdir),
        'exit_code_on_out_of_memory': 1,
        'third_dir_path': third_dir,
        'path_in_third': 'lingpipe-4.10',
        'offset_types': ['BYTES'],
        'offset_debugging': True,
        'cleanup_tmp_files': False,
        'align_labels_by': 'byte_offset_labels',
        'aligner_data': {
            'annotator_id': 'author',
            'tagger_id': 'lingpipe',
        },
    })
    lp.process_path(c_path)
    ## this are only present if cleanup_tmp_files is False
    assert tmpdir.join('chunk.sc-clean_visible.xml').read()
    assert tmpdir.join('chunk.sc-ner.xml').read()
    si = list(streamcorpus.Chunk(c_path))[0]
    assert len(si.body.clean_visible) > 200
    assert len(si.body.sentences['lingpipe']) == 41

@pytest.mark.integration
def test_aligner_separate(tmpdir, request, test_data_dir, third_dir):


    si = make_hyperlink_labeled_test_stream_item(test_data_dir)
    assert len(si.body.clean_visible) > 200
    #for x in si.body.labels['author']:
    #    print x.offsets[OffsetType.BYTES].first, x.offsets[OffsetType.BYTES].value, x.target.target_id
    c_path = str(tmpdir.join('chunk.sc'))
    chunk = streamcorpus.Chunk(c_path, mode='wb')
    chunk.add(si)
    chunk.close()

    lp = lingpipe(config={
        'tmp_dir_path': str(tmpdir),
        'exit_code_on_out_of_memory': 1,
        'third_dir_path': third_dir,
        'path_in_third': 'lingpipe-4.10',
        'offset_types': ['BYTES'],
        'offset_debugging': True,
        'cleanup_tmp_files': False,
    })
    lp.process_path(c_path)
    assert tmpdir.join('chunk.sc-clean_visible.xml').read()
    assert tmpdir.join('chunk.sc-ner.xml').read()

    ## run the aligner separately
    aligner = byte_offset_align_labels(config={
        'annotator_id': 'author',
        'tagger_id': 'lingpipe',
    })
    aligner.process_path(c_path)

    ## verify that we get the same answer as test above
    si = list(streamcorpus.Chunk(c_path))[0]
    assert len(si.body.clean_visible) > 200
    assert len(si.body.sentences['lingpipe']) == 41
