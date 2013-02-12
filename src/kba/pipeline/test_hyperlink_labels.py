import pytest 
import os
import time
from streamcorpus import StreamItem, ContentItem, OffsetType
from _hyperlink_labels import anchors_re, hyperlink_labels

def test_basics():
    stream_item = StreamItem()
    stream_item.body = ContentItem()
    path = os.path.dirname(__file__)
    path = os.path.join( path, '../../../data/test/' )
    stream_item.body.clean_html = open(
        os.path.join(path, 'nytimes-index-clean.html')).read()

    start = time.time()
    ## run it with a byte state machine
    hyperlink_labels(
        {'require_abs_url': True, 
         'domain_substrings': ['nytimes.com'],
         'all_domains': False,
         'offset_types': ['BYTES']}
        )(stream_item)
    elapsed_state_machine = time.time() - start

    assert stream_item.body.labelsets[0].labels[0].offsets.keys() == [OffsetType.BYTES]

    start = time.time()
    ## run it with regex
    hyperlink_labels(
        {'require_abs_url': True, 
         'domain_substrings': ['nytimes.com'],
         'all_domains': False,
         'offset_types': ['LINES']}
        )(stream_item)
    elapsed_re = time.time() - start

    assert stream_item.body.labelsets[1].labels[0].offsets.keys() == [OffsetType.LINES]

    line_labels = set()
    byte_labels = set()
    for labelset in stream_item.body.labelsets:
        for label in labelset.labels:
            if OffsetType.BYTES in label.offsets:
                byte_labels.add(label.target_id)
            if OffsetType.LINES in label.offsets:
                line_labels.add(label.target_id)

    assert line_labels == byte_labels
    print '\n\n%.5f statemachine based,\n %.5f regex based' % (elapsed_state_machine, elapsed_re)



@pytest.mark.parametrize(('parser_type',), [
    ('BYTES',),
    ('LINES',),
])
def test_speed(parser_type):
    stream_items = []
    for i in xrange(10):
        stream_item = StreamItem()
        stream_item.body = ContentItem()
        path = os.path.dirname(__file__)
        path = os.path.join( path, '../../../data/test/' )
        stream_item.body.clean_html = open(
            os.path.join(path, 'nytimes-index-clean.html')).read()
        stream_items.append( stream_item )

    start = time.time()
    ## run it with a byte state machine
    for si in stream_items:
        si = hyperlink_labels(
            {'require_abs_url': True, 
             'domain_substrings': ['nytimes.com'],
             'all_domains': False,
             'offset_types': [parser_type]}
            )(si)
    elapsed = time.time() - start
    
    rate = len(stream_items) / elapsed

    print OffsetType
    print '\n\n%.1f per second for %s' % (rate, parser_type)


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


@pytest.mark.xfail
def test_bytes_long_doc():
    stream_item = StreamItem()
    stream_item.body = ContentItem()
    path = os.path.dirname(__file__)
    path = os.path.join( path, '../../../data/test/' )
    stream_item.body.clean_html = open(
        os.path.join(path, 'company-test.html')).read()

    ## run it with a byte state machine
    hyperlink_labels(
        {'require_abs_url': True, 
         'all_domains': True,
         'offset_types': ['BYTES']}
        )(stream_item)

    
