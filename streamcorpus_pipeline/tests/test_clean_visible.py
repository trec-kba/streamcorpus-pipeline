from __future__ import absolute_import
from streamcorpus_pipeline._clean_visible import cleanse, make_clean_visible


def test_cleanse():
    assert (cleanse(u'This -LRB-big-RRB- dog has no \u1F601 Teeth') ==
            u'this big dog has no \u1F601 teeth')


def test_make_clean_visible_simple():
    s = 'The quick brown fox jumped over the lazy dog.'
    t = 'The quick brown fox jumped over the lazy dog.'
    u = make_clean_visible(s)
    assert t == u


def test_make_clean_visible_tag():
    s = 'The <i>quick</i> brown fox <b>jumped</b> over the lazy dog.'
    t = 'The    quick     brown fox    jumped     over the lazy dog.'
    u = make_clean_visible(s)
    assert t == u


def test_make_clean_visible_nl():
    s = 'The <i>quick\nbrown\nfox</i> jumped\nover the <b>lazy\rdog</b>.'
    t = 'The    quick\nbrown\nfox     jumped\nover the    lazy\rdog    .'
    u = make_clean_visible(s)
    assert t == u


def test_make_clean_visible_embedded_nl():
    s = 'The <a href="fox#quick">fox</a> <a\nhref="jump">jumped</a>.'
    t = 'The                     fox       \n            jumped    .'
    u = make_clean_visible(s)
    assert t == u


def test_make_clean_visible_script():
    s = 'The <script><![CDATA[crafty]]></script> fox jumped.'
    t = 'The                                     fox jumped.'
    u = make_clean_visible(s)
    assert t == u


def test_make_clean_visible_script_2():
    s = 'The <script>crafty</script> fox jumped.'
    t = 'The         crafty          fox jumped.'
    u = make_clean_visible(s)
    assert t == u
