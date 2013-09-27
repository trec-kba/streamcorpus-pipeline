
import os
from stages import _init_stage
from streamcorpus import make_stream_item, ContentItem

def test_langauge():
    path = os.path.dirname(__file__)
    path = os.path.join(path, '../../../data/test/raw-unicode-issues.html')
    si = make_stream_item(None, 'test')
    si.body = ContentItem(raw=open(path).read())

    lang = _init_stage('language', {})
    context = {}
    lang(si, context)

    assert si.body.language.name == 'Japanese'
    assert si.body.language.code == 'ja'
