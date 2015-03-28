'''tests for nilsimsa transform

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2015 Diffeo, Inc.
'''
from __future__ import absolute_import
import os

from nilsimsa import Nilsimsa
from streamcorpus import make_stream_item, ContentItem, Chunk

from streamcorpus_pipeline._clean_html import clean_html
from streamcorpus_pipeline._clean_visible import cleanse, clean_visible
from streamcorpus_pipeline._nilsimsa import nilsimsa
from streamcorpus_pipeline.tests._test_data import get_test_v0_3_0_chunk_path

def test_nilsimsa(test_data_dir):
    path = get_test_v0_3_0_chunk_path(test_data_dir)
    si = iter(Chunk(path)).next()
    assert si.body.raw

    context = {}

    ch = clean_html(config={})
    ch(si, context)
    assert si.body.clean_html

    cv = clean_visible(config={})
    cv(si, context)
    assert si.body.clean_visible

    xform = nilsimsa(config={})
    xform(si, context)

    text = cleanse(si.body.clean_visible.decode('utf8')).encode('utf8')
    assert si.other_content['nilsimsa'].raw == Nilsimsa(text).hexdigest()
