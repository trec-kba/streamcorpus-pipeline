from __future__ import absolute_import
import os

import pytest

import streamcorpus_pipeline
from streamcorpus_pipeline._yaml_files_list import yaml_files_list
import yakonfig

@pytest.yield_fixture(scope="function")
def yfl():
    yaml = """
streamcorpus_pipeline:
  yaml_files_list: {}
"""
    with yakonfig.defaulted_config([streamcorpus_pipeline], yaml=yaml,
                                   validate=False):
        yield yaml_files_list()

def test_parse_file(yfl):
    input_file = os.path.join(os.path.dirname(__file__), '../../data/john-smith/ground-truth.yaml')
    cnt = 0
    for si in yfl(input_file):
        assert si.stream_id
        assert si.body.raw
        cnt += 1
    assert cnt == 197

def test_parse_mentions(yfl):
    raw_mentions = ['John Smith', {'name': 'John Smith'}, {'ip_address': '10.0.0.1'}]
    mentions = yfl._parse_mentions(raw_mentions)

    assert len(mentions) == len(raw_mentions)
    assert mentions == [('name', 'John Smith'), ('name', 'John Smith'), ('ip_address', '10.0.0.1')]
