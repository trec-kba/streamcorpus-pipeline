from __future__ import absolute_import

import logging
import os
import uuid
import shutil

import pytest

import streamcorpus
import streamcorpus_pipeline
from streamcorpus import OffsetType
from streamcorpus_pipeline._lingpipe import only_whitespace
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

def test_aligner(tmpdir, request):

    si = make_hyperlink_labeled_test_stream_item()
    assert len(si.body.clean_visible) > 200
    #for x in si.body.labels['author']:
    #    print x.offsets[OffsetType.BYTES].first, x.offsets[OffsetType.BYTES].value, x.target.target_id
    c_path = str(tmpdir.join('chunk.sc'))
    chunk = streamcorpus.Chunk(c_path, mode='wb')
    chunk.add(si)
    chunk.close()

    config_yaml = """
streamcorpus_pipeline:
    lingpipe:
        tmp_dir_path: {!s}
        exit_code_on_out_of_memory: 1
        pipeline_root_path: {!s}
        offset_types: [BYTES]
        offset_debugging: True
        cleanup_tmp_files: False
        align_labels_by: byte_offset_labels
        aligner_data:
            annotator_id: author
            tagger_id: lingpipe
""".format(tmpdir, request.fspath.join('..', '..', '..', 'third'))
    with yakonfig.defaulted_config([streamcorpus_pipeline], yaml=config_yaml,
                                   validate=False):
        lp = streamcorpus_pipeline.stages._init_stage('lingpipe')
        logger.debug(c_path)
        lp.process_path(c_path)
        ## this are only present if cleanup_tmp_files is False
        assert tmpdir.join('chunk.sc-clean_visible.xml').read()
        assert tmpdir.join('chunk.sc-ner.xml').read()
        logger.debug(os.listdir(str(tmpdir)))
        si = list(streamcorpus.Chunk(c_path))[0]
        logger.info('%d bytes clean_visible for %s', len(si.body.clean_visible), si.stream_id)
        assert len(si.body.clean_visible) > 200
        logger.info('%d sentences for %s', len(si.body.sentences['lingpipe']), si.stream_id)
        assert len(si.body.sentences['lingpipe']) == 41, tmpdir.join('chunk.sc-ner.xml').read()


def test_aligner_separate(tmpdir, request):


    si = make_hyperlink_labeled_test_stream_item()
    assert len(si.body.clean_visible) > 200
    #for x in si.body.labels['author']:
    #    print x.offsets[OffsetType.BYTES].first, x.offsets[OffsetType.BYTES].value, x.target.target_id
    c_path = str(tmpdir.join('chunk.sc'))
    chunk = streamcorpus.Chunk(c_path, mode='wb')
    chunk.add(si)
    chunk.close()

    config_yaml = """
streamcorpus_pipeline:
    lingpipe:
        tmp_dir_path: {!s}
        exit_code_on_out_of_memory: 1
        pipeline_root_path: {!s}
        offset_types: [BYTES]
        offset_debugging: True
        cleanup_tmp_files: False
    byte_offset_align_labels:
        annotator_id: author
        tagger_id: lingpipe
""".format(tmpdir, request.fspath.join('..', '..', '..', 'third'))
    with yakonfig.defaulted_config([streamcorpus_pipeline], yaml=config_yaml,
                                   validate=False):
        lp = streamcorpus_pipeline.stages._init_stage('lingpipe')
        logger.debug(c_path)
        lp.process_path(c_path)
        assert tmpdir.join('chunk.sc-clean_visible.xml').read()
        assert tmpdir.join('chunk.sc-ner.xml').read()
        logger.debug(os.listdir(str(tmpdir)))

        ## run the aligner separately
        aligner = streamcorpus_pipeline.stages._init_stage(
            'byte_offset_align_labels')
        aligner.process_path(c_path)

        ## verify that we get the same answer as test above
        si = list(streamcorpus.Chunk(c_path))[0]
        logger.info('%d bytes clean_visible for %s', len(si.body.clean_visible), si.stream_id)
        assert len(si.body.clean_visible) > 200
        logger.info('%d sentences for %s', len(si.body.sentences['lingpipe']), si.stream_id)
        assert len(si.body.sentences['lingpipe']) == 41, tmpdir.join('chunk.sc-ner.xml').read()

