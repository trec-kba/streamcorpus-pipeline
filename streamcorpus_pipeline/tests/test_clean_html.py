# coding: utf-8

import os
import pytest
import sys
import time

from streamcorpus import StreamItem, ContentItem
from streamcorpus_pipeline._clean_html import make_clean_html, clean_html
from streamcorpus_pipeline._clean_visible import make_clean_visible
from streamcorpus_pipeline._hyperlink_labels import hyperlink_labels

# The output of this test is highly dependent on the version of libxml2
# installed on the system, and different environments produce different
# "generated" results.
@pytest.mark.skipif('True')
def test_make_clean_html_nyt(test_data_dir, tmpdir):
    path = os.path.join(test_data_dir, 'test')
    generated = make_clean_html(open(os.path.join(path, 'nytimes-index.html')).read().decode('utf8'))
    stable    = open(os.path.join(path, 'nytimes-index-clean-stable.html')).read()

    if generated != stable:
        outf = os.path.join(path, 'nytimes-index-clean-new-{0}.html'.format(time.strftime('%Y%m%d_%H%M%S')))
        sys.stderr.write('writing unmatching output to {0!r}\n'.format(outf))
        sys.stderr.write('diff -u {0} {1}\n'.format(os.path.join(path, 'nytimes-index-clean-stable.html'), outf))
        with open(outf, 'wb') as fout:
            fout.write(generated)
    assert generated == stable

    assert '<script' not in generated


def test_make_clean_html():
    test_bad_html = '''
<a href="http://birdingblogs.com/author/daleforbes">birdingblogs.com</a></div><div
id="comments-template"><h3 id="comments">4 Responses to &#822050+ Years of Digiscoping History.&#8221;</h3>'''

    correct_test_bad_html = u'''<html><head><meta charset="utf-8"></head><body>
<a href="http://birdingblogs.com/author/daleforbes">birdingblogs.com</a><div><h3>4 Responses to   + Years of Digiscoping History.”</h3></div>

</body></html>
'''

    email_html_1 = '''
<html><head/><body>This and that <user@email.com>  </body></html>
'''
    corrected_email_html_1 = '''<html><head><meta charset="utf-8"></head>
<body>This and that &lt;user@email.com&gt;  ''' + '''

</body></html>'''

    ## split things up around the "+" because different versions of
    ## lxml insert different numbers of spaces!
    correct_first_half, correct_second_half = correct_test_bad_html.split('+')
    correct_first_half = correct_first_half.strip()
    correct_second_half = correct_second_half.strip()

    cleaned_email_1 =  make_clean_html(email_html_1)
    assert cleaned_email_1 == corrected_email_html_1

    cleaned =  make_clean_html(test_bad_html).decode('utf-8')
    cleaned_first_half, cleaned_second_half = cleaned.split('+')
    cleaned_first_half = cleaned_first_half.strip()
    cleaned_second_half = cleaned_second_half.strip()
    #assert cleaned == correct_test_bad_html, cleaned
    assert cleaned_first_half == correct_first_half and cleaned_second_half == correct_second_half, cleaned


def test_clean_html_simple_unicode():
    html = '<html><body>1☃</body></html>'
    si = StreamItem(body=ContentItem(raw=html, encoding='utf-8'))
    cleaned = make_clean_html(html, si)
    assert u'1' in cleaned.decode('utf-8')
    assert u'☃' in cleaned.decode('utf-8')


def test_clean_html_attr_strip():
    html = '<html><body><span class="foo" id="bar">foobar</span></body></html>'
    si = StreamItem(body=ContentItem(raw=html, encoding='utf-8'))
    cleaned = make_clean_html(html, si)
    assert cleaned.decode('utf-8') == '''
<html><head><meta charset="utf-8"></head><body><span>foobar</span>
</body></html>'''.strip()


def test_clean_html_remove_base():
    html = '<html><body><base href="http://foo.com"><p>foo</p></body></html>'
    si = StreamItem(body=ContentItem(raw=html, encoding='utf-8'))
    cleaned = make_clean_html(html, si)
    assert cleaned.decode('utf-8') == '''
<html><head><meta charset="utf-8"></head><body><p>foo</p>
</body></html>'''.strip()


def test_target_parsing(test_data_dir):
    path = os.path.join(test_data_dir, 'test')
    test_html = open(os.path.join(path, 'target-test.html')).read()

    html = make_clean_html( test_html )

    assert 'charset="utf-8"' in html
    assert 'target' in html

    visible = make_clean_visible( html )

    assert 'charset="utf-8"' not in visible
    assert 'target' not in visible

    stage = hyperlink_labels(config={
        'offset_types': ['CHARS'],
        'require_abs_url': True,
        'all_domains': True,
    })
    si = StreamItem(body=ContentItem(clean_html=html))
    context = {}
    stage( si, context )
    html2 = si.body.clean_html

    visible2 = make_clean_visible( html2 )

    print visible2

    assert 'target' not in visible2
    assert 'charset="utf-8"' not in visible2


@pytest.mark.xfail  # pylint: disable=E1101
def test_unicode_conversion(test_data_dir):
    path = os.path.join(test_data_dir, 'test')
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

@pytest.mark.skipif('True')
def test_stage(test_data_dir):
    stage = clean_html({}) # NB: not even defaults
    path = os.path.join(test_data_dir, 'test')
    with open(os.path.join(path, 'nytimes-index.html'), 'r') as f:
        raw = f.read().decode('utf8')
    si = StreamItem(body=ContentItem(raw=raw, media_type='text/html'))
    si = stage(si, {})

    with open(os.path.join(path, 'nytimes-index-clean-stable.html'), 'r') as f:
        stable = f.read()
    if si.body.clean_html != stable:
        outf = os.path.join(path, 'nytimes-index-clean-new-{0}.html'.format(time.strftime('%Y%m%d_%H%M%S')))
        sys.stderr.write('writing unmatching output to {0!r}\n'.format(outf))
        sys.stderr.write('diff -u {0} {1}\n'.format(os.path.join(path, 'nytimes-index-clean-stable.html'), outf))
        with open(outf, 'wb') as fout:
            fout.write(si.body.clean_html)
    assert si.body.clean_html == stable
