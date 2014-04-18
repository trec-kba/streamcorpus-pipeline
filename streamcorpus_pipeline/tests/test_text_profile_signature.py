
from __future__ import absolute_import
import os
import hashlib
from streamcorpus_pipeline.text_profile_signature import tps

def test_tps(test_data_dir):
    path = os.path.join(test_data_dir, 'test')
    text1 = open(os.path.join(path, 'nytimes-index-clean-visible.html')).read().decode('utf8')
    print hashlib.md5(repr(text1)).hexdigest(), tps(text1)
    text2 = open(os.path.join(path, 'nytimes-index-clean-visible-dup.html')).read().decode('utf8')
    print hashlib.md5(repr(text2)).hexdigest(), tps(text2)
