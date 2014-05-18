from __future__ import absolute_import
import os

import pytest

import streamcorpus
from streamcorpus_pipeline._upgrade_streamcorpus_v0_3_0 import \
    upgrade_streamcorpus_v0_3_0
from streamcorpus_pipeline.tests._test_data import get_test_chunk_path

def test_protection(test_data_dir):
    with pytest.raises(streamcorpus.VersionMismatchError):  # pylint: disable=E1101
        for si in streamcorpus.Chunk(
            os.path.join(
                test_data_dir,
                'test/MAINSTREAM_NEWS-15-9d6218f0aa7c9585cda12a10d642a8b3-41600ffca7703f7914102da5256233ce.sc.xz'),
            message=streamcorpus.StreamItem
            ):
            pass

def test_upgrade_streamcorpus_v0_3_0(test_data_dir):
    up = upgrade_streamcorpus_v0_3_0(config={})
    count = 0
    
    for si in streamcorpus.Chunk(get_test_chunk_path(test_data_dir), message=streamcorpus.StreamItem_v0_2_0):
        count += 1
        si3 = up(si)
        assert si3.version == streamcorpus.Versions._NAMES_TO_VALUES['v0_3_0']
        if count > 10:
            break

def test_upgrade_streamcorpus_v0_3_0_check_mention_ids(test_data_dir):
    up = upgrade_streamcorpus_v0_3_0(config={})
    all_mention_ids = set()
    for si in streamcorpus.Chunk(
            os.path.join(
                test_data_dir,
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
