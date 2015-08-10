from __future__ import absolute_import
from streamcorpus_pipeline._clean_visible import cleanse, \
    make_clean_visible_from_raw, \
    make_clean_visible


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


def test_make_clean_visible_email():
    s = 'The <i>quick\nbrown\nfox<user@email.com></i> jumped\n over the <b><lazy@dog.com> lazy\rdog</b>.'
    t = 'The    quick\nbrown\nfox&lt;user@email.com&gt;     jumped\n over the    &lt;lazy@dog.com&gt; lazy\rdog    .'
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


def test_make_clean_visible_from_raw_link():
    s = 'The <link></link> fox jumped.'
    t = 'The               fox jumped.'
    u = make_clean_visible_from_raw(s)
    assert t == u


def test_make_clean_visible_from_raw_link_2():
    s = 'The <link/> fox jumped.'
    t = 'The         fox jumped.'
    u = make_clean_visible_from_raw(s)
    assert t == u


def test_make_clean_visible_from_raw_script():
    s = 'The <script></script> fox jumped.'
    t = 'The                   fox jumped.'
    u = make_clean_visible_from_raw(s)
    assert t == u


def test_make_clean_visible_from_raw_script_2():
    s = 'The <script>crafty</script> fox jumped.'
    t = 'The                         fox jumped.'
    u = make_clean_visible_from_raw(s)
    assert t == u


def test_make_clean_visible_from_raw_script_3():
    s = 'The <script>crafty\n\n<more tags>\nand crazy stuff</ and other> stuff </style</script> fox jumped.'
    t = 'The               \n\n           \n                                                    fox jumped.'
    u = make_clean_visible_from_raw(s)
    assert t == u


def test_make_clean_visible_from_raw_script_4():
    s = 'The <script>crafty\n\n<style>\nand crazy\n</style> stuff</ and other> stuff </style</script> fox jumped.'
    t = 'The               \n\n       \n         \n                                                   fox jumped.'
    u = make_clean_visible_from_raw(s)
    assert t == u


def test_make_clean_visible_from_raw_script_5():
    s = 'The <script language="JavaScript">crafty\n\n<style>\nand crazy\n</style> stuff</ and other> stuff </style</script> fox jumped.'
    t = 'The                                     \n\n       \n         \n                                                   fox jumped.'
    u = make_clean_visible_from_raw(s)
    assert t == u


def test_make_clean_visible_from_raw_script_6():
    s = 'The <script\n language="JavaScript">crafty\n\n<style>\nand crazy\n</style> stuff</ and other> stuff </style</script> fox jumped.'
    t = 'The        \n                             \n\n       \n         \n                                                   fox jumped.'
    u = make_clean_visible_from_raw(s)
    assert t == u


def test_make_clean_visible_from_raw_style():
    s = 'The <style>crafty\n\n<style>\nand crazy\n</notstyle> stuff</ and other> stuff </style</style> fox jumped.'
    t = 'The              \n\n       \n         \n                                                     fox jumped.'
    u = make_clean_visible_from_raw(s)
    assert t == u


def test_make_clean_visible_from_raw_comment_1():
    s = 'The <!---->a<!-- --> hi <!--\n crafty\n\n<style>\nand crazy\n</notstyle> st-->uff</ and other> stuff<!-- </style</style> fox--> jumped.'
    t = 'The        a         hi     \n       \n\n       \n         \n                 uff              stuff                            jumped.'
    u = make_clean_visible_from_raw(s)
    assert t == u


def test_make_clean_visible_from_raw_comment_2():
    s = 'The <script language="JS"> \n <!a<!-- --> hi <!--\n</script> fox--> jumped.'
    t = 'The                        \n                    \n          fox--> jumped.'
    u = make_clean_visible_from_raw(s)
    assert t == u


def test_make_clean_visible_from_raw_ending_newlines():
    s = '<html>The\n <tags> galor </so much> jumped.</html>\n\n'
    t = '      The\n        galor            jumped.       \n\n'
    u = make_clean_visible_from_raw(s)
    assert t == u
