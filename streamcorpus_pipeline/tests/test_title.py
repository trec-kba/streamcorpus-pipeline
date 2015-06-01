# -*- coding: utf-8 -*-

from __future__ import absolute_import, division, print_function

from streamcorpus import make_stream_item
from streamcorpus_pipeline._title import title
from streamcorpus_pipeline._clean_visible import clean_visible

def test_title():
    stage = title({})
    cv = clean_visible({})

    si = make_stream_item(0, '')
    si.body.clean_html = '''Then there
was a
<tag>   ...  <title>TITLE
HERE
  </title>
'''
    si = cv(si, {})
    si = stage(si)
    assert si.other_content['title'].clean_visible == 'TITLE HERE'

    si = make_stream_item(0, '')
    si.body.clean_html = '''Then there
was a
  that went <tag>   ...  <title>TITLE
HERE%s
  </title>
''' % ('*' * 80)
    si = cv(si, {})
    si = stage(si)
    assert si.other_content['title'].clean_visible == 'TITLE HERE' + '*' * 50 + '...'


def test_title_unicode_clean_html():
    # The trick is to make the 60th byte end at a non-character boundary.
    # Use the snowman (3 byte encoding) and some foreign character (4 byte
    # encoding). Since 7 does not divide 60, the 60th byte will be in the
    # middle of an encoded codepoint.
    foreign_snowmen = u'☃𠜎' * 100
    stage = title({})
    cv = clean_visible({})

    si = make_stream_item(0, '')
    si.body.clean_html = '<title>%s</title>' % foreign_snowmen.encode('utf-8')
    si = cv(si, {})
    si = stage(si)
    got = si.other_content['title'].clean_visible.decode('utf-8')
    assert len(foreign_snowmen[0:60]) + 3 == len(got)
    assert foreign_snowmen[0:60] + '...' == got


def test_title_unicode_clean_visible():
    # The trick is to make the 200th byte end at a non-character boundary.
    # Use the snowman (3 byte encoding) and some foreign character (4 byte
    # encoding). Since 7 does not divide 200, the 200th byte will be in the
    # middle of an encoded codepoint.
    foreign_snowmen = u'☃𠜎' * 100
    stage = title({})
    cv = clean_visible({})

    si = make_stream_item(0, '')
    si.body.clean_html = None
    si.body.clean_visible = '%s' % foreign_snowmen.encode('utf-8')
    si = stage(si)
    got = si.other_content['title'].clean_visible.decode('utf-8')
    assert len(foreign_snowmen[0:60]) + 3 == len(got)
    assert foreign_snowmen[0:60] + '...' == got
