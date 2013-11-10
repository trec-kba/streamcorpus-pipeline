import os
import pytest
import streamcorpus
from streamcorpus_pipeline.stages import _init_stage
from tests.streamcorpus_pipeline._test_data import _TEST_DATA_ROOT

def test_protection():
    with pytest.raises(streamcorpus.VersionMismatchError):  # pylint: disable=E1101
        for si in streamcorpus.Chunk(
            os.path.join(
                os.path.dirname(__file__),
                _TEST_DATA_ROOT,
                'test/MAINSTREAM_NEWS-15-9d6218f0aa7c9585cda12a10d642a8b3-41600ffca7703f7914102da5256233ce.sc.xz'),
            message=streamcorpus.StreamItem
            ):
            pass

def test_upgrade_streamcorpus_v0_3_0():

    up = _init_stage('upgrade_streamcorpus_v0_3_0', {})

    count = 0
    for si in streamcorpus.Chunk(
        os.path.join(
            os.path.dirname(__file__),
            _TEST_DATA_ROOT,
            'test/WEBLOG-100-fd5f05c8a680faa2bf8c55413e949bbf.sc'),
        message=streamcorpus.StreamItem_v0_2_0
        ):
        
        count += 1

        si3 = up(si)

        assert si3.version == streamcorpus.Versions._NAMES_TO_VALUES['v0_3_0']

        if count > 10:
            break


def test_upgrade_streamcorpus_v0_3_0_check_mention_ids():

    up = _init_stage('upgrade_streamcorpus_v0_3_0', {})

    all_mention_ids = set()

    for si in streamcorpus.Chunk(
        os.path.join(
            os.path.dirname(__file__),
            _TEST_DATA_ROOT,
            'test/MAINSTREAM_NEWS-15-9d6218f0aa7c9585cda12a10d642a8b3-41600ffca7703f7914102da5256233ce.sc.xz'),
        message=streamcorpus.StreamItem_v0_2_0
        ):
        
        si3 = up(si)

        assert si3.version == streamcorpus.Versions._NAMES_TO_VALUES['v0_3_0']

        mention_ids = set()
        for sentence in si3.body.sentences['lingpipe']:
            sentence_mention_ids = set()
            for token in sentence.tokens:
                if token.mention_id not in [None, -1]:
                    sentence_mention_ids.add(token.mention_id)

            assert mention_ids.intersection(sentence_mention_ids) == set()

            mention_ids.update( sentence_mention_ids )

            all_mention_ids.update( sentence_mention_ids )

    assert len(all_mention_ids) > 0

