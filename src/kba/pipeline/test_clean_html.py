import os
import pytest

from streamcorpus import StreamItem, ContentItem
from _stages import _init_stage
from _clean_html import make_clean_html
from _clean_visible import make_clean_visible

def test_make_clean_html():
    path = os.path.dirname(__file__)
    path = os.path.join( path, '../../../data/test/' )
    open(os.path.join(path, 'nytimes-index-clean.html'), 'wb').write(
        make_clean_html(open(os.path.join(path, 'nytimes-index.html')).read()))

    test_bad_html = '''
<a href="http://birdingblogs.com/author/daleforbes">birdingblogs.com</a></div><div
id="comments-template"><h3 id="comments">4 Responses to &#822050+ Years of Digiscoping History.&#8221;</h3>'''

    correct_test_bad_html = '''<html><body><div>
<a href="http://birdingblogs.com/author/daleforbes">birdingblogs.com</a><div id="comments-template"><h3 id="comments">4 Responses to   + Years of Digiscoping History.”</h3></div>
</div></body></html>
'''

    cleaned =  make_clean_html(test_bad_html)
    assert cleaned == correct_test_bad_html, cleaned

def test_target_parsing():
    path = os.path.dirname(__file__)
    path = os.path.join( path, '../../../data/test/' )
    test_html = open(os.path.join(path, 'target-test.html')).read()

    html = make_clean_html( test_html )

    assert 'logo' in html
    assert 'target' in html

    visible = make_clean_visible( html )
    
    assert 'logo' not in visible
    assert 'target' not in visible

    hyperlink_labels = _init_stage(
        'hyperlink_labels', 
        dict(offset_types=['LINES'],
             require_abs_url=True,
             all_domains=True,
             ))
    si = StreamItem(body=ContentItem(clean_html=html))
    hyperlink_labels( si )
    html2 = si.body.clean_html

    visible2 = make_clean_visible( html2 )
    
    #print visible2

    assert 'target' not in visible2
    assert 'logo' not in visible2



@pytest.mark.xfail
def test_unicode_conversion():
    path = os.path.dirname(__file__)
    path = os.path.join( path, '../../../data/test/' )
    test_html = open(os.path.join(path, 'raw-unicode-issues.html')).read()

    print type(test_html)
    print test_html
    print repr(test_html)
    print str(test_html).decode('utf8')

    html = make_clean_html( test_html )

    print unicode(html)

    visible = make_clean_visible( html )

    print type(visible)

    print visible.decode('utf8')
