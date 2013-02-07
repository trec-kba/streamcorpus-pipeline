import os
from streamcorpus import StreamItem, ContentItem
from _hyperlink_labels import anchors_re, hyperlink_labels

def test_basics():
    stream_item = StreamItem()
    stream_item.body = ContentItem()
    path = os.path.dirname(__file__)
    path = os.path.join( path, '../../../data/test/' )
    stream_item.body.clean_html = open(
        os.path.join(path, 'nytimes-index-clean.html')).read()
    stream_item = hyperlink_labels(
        {'require_abs_url': True, 
         'domain_substrings': ['nytimes.com']}
        )(stream_item)
    assert len(stream_item.body.labelsets[0].labels) == 373

sample_text = u'''
This is a normal <a href="http://en.wikipedia.org/wiki/Hello">link</a>.

This is a funky <a
href ='http://en.wikipedia.org/wiki/Bogus'  asdf=4
>the Bogus</a>

              Obviously intrigued by anything named Munn I sought <a href="http://en.wikipedia.org/wiki/Munny">more 
              info</a>.</font></p>

'''

def test_anchors_re():
    matches = list(anchors_re.finditer(sample_text))
    for m in matches:
        before = m.group('before')
        href = m.group('href')
        ahref = m.group('ahref')
        posthref = m.group('posthref')
        preequals = m.group('preequals')
        postequals = m.group('postequals')
        #print m.groups('href')

    ## now we check more of the output
    assert len(matches) == 3

