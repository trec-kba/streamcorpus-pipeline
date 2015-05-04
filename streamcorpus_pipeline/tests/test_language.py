'''tests for langauge detection transform

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2015 Diffeo, Inc.
'''
from __future__ import absolute_import
import os
import pytest
import streamcorpus_pipeline
from streamcorpus_pipeline._clean_html import clean_html
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


@pytest.mark.parametrize('with_clean_html', [(True,), (False,)])
def test_language_unreliable_on_raw(test_data_dir, with_clean_html):
    path = os.path.join(test_data_dir, 'test/unreliable-language-detect-on-raw.html')
    si = make_stream_item(None, 'http://bbs.sjtu.edu.cn/bbsanc?path=%2Fgroups%2FGROUP_0%2Fmessage%2FD4EFC2634%2FD7AC8E3A8%2FG.1092960050.A')
    raw = open(path).read()
    #raw = raw.decode('GB2312', 'ignore').encode('utf8')
    si.body = ContentItem(raw=raw)
    si.body.encoding = 'GB2312'
    si.body.media_type = 'text/html'
    context = {}
    if with_clean_html:
        ch = clean_html(config={})
        ch(si, context)
    lang = language(config={})
    lang(si, context)

    assert si.body.language.name == 'Chinese'
    assert si.body.language.code == 'zh'
