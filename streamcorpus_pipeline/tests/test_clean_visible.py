import os
import pytest

from streamcorpus_pipeline._clean_visible import cleanse

def test_cleanse():
    assert cleanse(u'This -LRB-big-RRB- dog has no \u1F601 Teeth') == u'this big dog has no \u1F601 teeth'
