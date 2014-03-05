from __future__ import absolute_import
import os
import streamcorpus_pipeline
from streamcorpus_pipeline._language import language
from streamcorpus import make_stream_item, ContentItem
from streamcorpus_pipeline.tests._test_data import _TEST_DATA_ROOT

def test_langauge():
    path = os.path.dirname(__file__)
    path = os.path.join(path, _TEST_DATA_ROOT, 'test/raw-unicode-issues.html')
    si = make_stream_item(None, 'test')
    si.body = ContentItem(raw=open(path).read())
    context = {}
    lang = language(config={})
    lang(si, context)

    assert si.body.language.name == 'Japanese'
    assert si.body.language.code == 'ja'
