import os
import pytest

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
