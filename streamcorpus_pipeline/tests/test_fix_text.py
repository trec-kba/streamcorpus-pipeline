from __future__ import absolute_import, division, print_function
import os.path as path

from streamcorpus import make_stream_item, ContentItem
from streamcorpus_pipeline._fix_text import fix_text

def test_fix_text(test_data_dir):
    fpath = path.join(test_data_dir, 'test/microsoft-quotes.txt')
    si = make_stream_item(None, 'test')
    si.body = ContentItem(raw=open(fpath).read())
    fixer = fix_text(config={'read_from': 'raw', 'write_to': 'clean_visible'})
    fixer(si, {})
    assert(si.body.clean_visible.strip() == 'Do not "quote me."')
