

import os
import uuid
import pytest
import shutil
import streamcorpus
import streamcorpus_pipeline
from streamcorpus import OffsetType
from streamcorpus_pipeline._lingpipe import only_whitespace
from test_hyperlink_labels import make_hyperlink_labeled_test_stream_item
from test_hyperlink_labels import make_hyperlink_labeled_test_chunk
from streamcorpus_pipeline._logging import logger


def test_only_whitespace():
    assert only_whitespace.match('\n\t     \t    \n\t \n  ')
    assert only_whitespace.match('\n')
    assert only_whitespace.match('')
    assert only_whitespace.match(u'\u200b')
    assert only_whitespace.match(u'\n\n   \u200b')
    assert not only_whitespace.match(u'\u200b foo')
    assert not only_whitespace.match('\n\nh  ')

def test_aligner():

    si = make_hyperlink_labeled_test_stream_item()
    #for x in si.body.labels['author']:
    #    print x.offsets[OffsetType.BYTES].first, x.offsets[OffsetType.BYTES].value, x.target.target_id

    path = os.path.join('/tmp', str(uuid.uuid1()))
    chunk = streamcorpus.Chunk(path, mode='wb')
    chunk.add(si)
    chunk.close()

    lp = streamcorpus_pipeline.stages._init_stage(
        'lingpipe', dict(
            exit_code_on_out_of_memory=1,
            pipeline_root_path=os.path.join(os.path.dirname(__file__), '../../../third/'),
            offset_types = ['BYTES'],
            offset_debugging = True,
            cleanup_tmp_files = False,
            align_labels_by = 'byte_offset_labels',
            aligner_data = dict(
                annotator_id = 'author',
                tagger_id = 'lingpipe'
                )))

    print path
    lp.process_path(path)

    os.unlink(path)

@pytest.mark.xfail  # pylint: disable=E1101
def test_aligner_bulk():

    tpath = make_hyperlink_labeled_test_chunk()

    lp = streamcorpus_pipeline.stages._init_stage(
        'lingpipe', dict(
            exit_code_on_out_of_memory=1,
            pipeline_root_path=os.path.join(os.path.dirname(__file__), '../../../third/'),
            offset_types = ['BYTES'],
            offset_debugging = True,
            cleanup_tmp_files = False,
            align_labels_by = 'byte_offset_labels',
            aligner_data = dict(
                annotator_id = 'author',
                tagger_id = 'lingpipe'
                )))

    lp.process_path(tpath)

    chunk = streamcorpus.Chunk(tpath, mode='rb')
    for si in chunk:
        pass

    os.unlink(tpath)
