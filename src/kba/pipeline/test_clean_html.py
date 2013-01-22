

from _clean_html import make_clean_html

def test_make_clean_html():
    open('data/test/nytimes-index-clean.html', 'wb').write(
        make_clean_html(open('data/test/nytimes-index.html').read()))

    test_bad_html = '''
<a href="http://birdingblogs.com/author/daleforbes">birdingblogs.com</a></div><div
id="comments-template"><h3 id="comments">4 Responses to &#822050+ Years of Digiscoping History.&#8221;</h3>'''

    correct_test_bad_html = '''<html><body><div>
<a href="http://birdingblogs.com/author/daleforbes">birdingblogs.com</a><div id="comments-template"><h3 id="comments">4 Responses to   + Years of Digiscoping History.”</h3></div>
</div></body></html>
'''

    cleaned =  make_clean_html(test_bad_html)
    assert cleaned == correct_test_bad_html, cleaned
