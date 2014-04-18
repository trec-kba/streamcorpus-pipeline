from __future__ import absolute_import
import os
import streamcorpus_pipeline
from streamcorpus_pipeline._language import language
from streamcorpus import make_stream_item, ContentItem

def test_langauge(test_data_dir):
    path = os.path.join(test_data_dir, 'test/raw-unicode-issues.html')
    si = make_stream_item(None, 'test')
    si.body = ContentItem(raw=open(path).read())
    context = {}
    lang = language(config={})
    lang(si, context)

    assert si.body.language.name == 'Japanese'
    assert si.body.language.code == 'ja'
