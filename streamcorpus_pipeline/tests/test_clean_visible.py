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


def test_make_clean_visible_from_raw_comment_in_quote():
    s = '<html><meta foo="The big<!-- you know where comment <script haha> -->">The fox jumped.</html>\n\n'
    t = '                                                                       The fox jumped.       \n\n'
    u = make_clean_visible_from_raw(s)
    assert t == u


example1_1 = '''				<!-- ENLACES_120x90_2015_EI -->
				<ins class="adsbygoogle"
				     style="display:inline-block;width:120px;height:90px"
				     data-ad-client="ca-pub-8118159680005837"
				     data-ad-slot="8414446812"></ins>
				<script>
				(adsbygoogle = window.adsbygoogle || []).push({});
				</script>
				<script async src="//pagead2.googlesyndication.com/pagead/js/adsbygoogle.js"></script>
				<!-- ENLACES_120x90_2015_EI -->
				<ins class="adsbygoogle"
				     style="display:inline-block;width:120px;height:90px"
				     data-ad-client="ca-pub-8118159680005837"
				     data-ad-slot="8414446812"></ins>
				<script>
				(adsbygoogle = window.adsbygoogle || []).push({});
				</script>
			</div> 	 

			

			<p style="margin-top:10px;padding-right:10px" align="justify"><div id="HOTWordsTxt" name="HOTWordsTxt" style="text-align:justify;"><!--AUGURE_NOTICIA_INICIO--><b>'''
example1_2 = '''River supo caer para despues levantarse y ganar la Libertadores</b> / <b>El Tiempo</b> / Jonathan Maidana, '''

truth1_1 = ' ' * len(example1_1)
truth1_2   = '''River supo caer para despues levantarse y ganar la Libertadores     /    El Tiempo     / Jonathan Maidana, '''

truth1 = truth1_1 + truth1_2

def test_make_clean_visible_from_raw_example1_1():
    u = make_clean_visible_from_raw(example1_1)
    assert truth1_1 == u.replace('\t', ' ').replace('\n', ' ')

def test_make_clean_visible_from_raw_example1_2():
    u = make_clean_visible_from_raw(example1_2)
    assert truth1_2 == u

def test_make_clean_visible_from_raw_example1():
    u = make_clean_visible_from_raw(example1_1 + example1_2)
    assert truth1 == u.replace('\t', ' ').replace('\n', ' ')


example2 = '''<link href='http://fonts.googleapis.com/css?family=Jura' rel='stylesheet' type='text/css'>
				<li>Principal
'''

def test_make_clean_visible_from_raw_example2():
    u = make_clean_visible_from_raw(example2)
    assert 'Principal' == u.strip().replace('\t', ' ').replace('\n', ' ')
