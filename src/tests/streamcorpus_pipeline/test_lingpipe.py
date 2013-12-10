

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

@pytest.fixture(scope='function')
def tmp_dir_path(request):
    path = os.path.join('/tmp', str(uuid.uuid4()))
    os.makedirs(path)
    def fin():
        shutil.rmtree(path)
    request.addfinalizer(fin)
    return path

def test_aligner(tmp_dir_path):

    si = make_hyperlink_labeled_test_stream_item()
    assert len(si.body.clean_visible) > 200
    #for x in si.body.labels['author']:
    #    print x.offsets[OffsetType.BYTES].first, x.offsets[OffsetType.BYTES].value, x.target.target_id
    c_path = os.path.join(tmp_dir_path, 'chunk.sc')
    chunk = streamcorpus.Chunk(c_path, mode='wb')
    chunk.add(si)
    chunk.close()

    lp = streamcorpus_pipeline.stages._init_stage(
        'lingpipe', dict(
            tmp_dir_path = tmp_dir_path,
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

    logger.critical(c_path)
    lp.process_path(c_path)
    assert os.path.exists(tmp_dir_path)
    ## this are only present if cleanup_tmp_files is False
    assert open(os.path.join(tmp_dir_path, 'chunk.sc-clean_visible.xml')).read()
    assert open(os.path.join(tmp_dir_path, 'chunk.sc-ner.xml')).read()
    logger.critical(os.listdir(tmp_dir_path))
    si = list(streamcorpus.Chunk(c_path))[0]
    logger.critical('%d bytes clean_visible for %s', len(si.body.clean_visible), si.stream_id)
    assert len(si.body.clean_visible) > 200
    logger.critical('%d sentences for %s', len(si.body.sentences['lingpipe']), si.stream_id)
    assert len(si.body.sentences['lingpipe']) == 41, open(os.path.join(tmp_dir_path, 'chunk.sc-ner.xml')).read()
    assert os.path.exists(tmp_dir_path)


def test_aligner_separate(tmp_dir_path):


    si = make_hyperlink_labeled_test_stream_item()
    assert len(si.body.clean_visible) > 200
    #for x in si.body.labels['author']:
    #    print x.offsets[OffsetType.BYTES].first, x.offsets[OffsetType.BYTES].value, x.target.target_id
    c_path = os.path.join(tmp_dir_path, 'chunk.sc')
    chunk = streamcorpus.Chunk(c_path, mode='wb')
    chunk.add(si)
    chunk.close()

    lp = streamcorpus_pipeline.stages._init_stage(
        'lingpipe', dict(
            tmp_dir_path = tmp_dir_path,
            exit_code_on_out_of_memory=1,
            pipeline_root_path=os.path.join(os.path.dirname(__file__), '../../../third/'),
            offset_types = ['BYTES'],
            offset_debugging = True,
            cleanup_tmp_files = False,
        )
    )
    logger.critical(c_path)
    lp.process_path(c_path)
    assert os.path.exists(tmp_dir_path)
    assert open(os.path.join(tmp_dir_path, 'chunk.sc-clean_visible.xml')).read()
    assert open(os.path.join(tmp_dir_path, 'chunk.sc-ner.xml')).read()
    logger.critical(os.listdir(tmp_dir_path))

    ## run the aligner separately
    aligner = streamcorpus_pipeline.stages._init_stage(
        'byte_offset_align_labels',
        dict(
            annotator_id = 'author',
            tagger_id = 'lingpipe'
        )
    )
    aligner.process_path(c_path)

    ## verify that we get the same answer as test above
    si = list(streamcorpus.Chunk(c_path))[0]
    logger.critical('%d bytes clean_visible for %s', len(si.body.clean_visible), si.stream_id)
    assert len(si.body.clean_visible) > 200
    logger.critical('%d sentences for %s', len(si.body.sentences['lingpipe']), si.stream_id)
    assert len(si.body.sentences['lingpipe']) == 41, open(os.path.join(tmp_dir_path, 'chunk.sc-ner.xml')).read()
    assert os.path.exists(tmp_dir_path)

