import pytest 
import os
import time
from streamcorpus import StreamItem, ContentItem, OffsetType
from _hyperlink_labels import anchors_re, hyperlink_labels

def make_test_stream_item():
    stream_item = StreamItem()
    stream_item.body = ContentItem()
    path = os.path.dirname(__file__)
    path = os.path.join( path, '../../../data/test/' )
    stream_item.body.clean_html = open(
        os.path.join(path, 'nytimes-index-clean.html')).read()
    return stream_item

def test_basics():
    start = time.time()
    ## run it with a byte state machine
    si1 = make_test_stream_item()
    hyperlink_labels(
        {'require_abs_url': True, 
         'domain_substrings': ['nytimes.com'],
         'all_domains': False,
         'offset_types': ['BYTES']}
        )(si1)
    elapsed_state_machine = time.time() - start

    assert si1.body.labels['author'][0].offsets.keys() == [OffsetType.BYTES]

    start = time.time()
    si2 = make_test_stream_item()
    ## run it with regex
    hyperlink_labels(
        {'require_abs_url': True, 
         'domain_substrings': ['nytimes.com'],
         'all_domains': False,
         'offset_types': ['LINES']}
        )(si2)
    elapsed_re = time.time() - start

    assert si2.body.labels['author'][0].offsets.keys() == [OffsetType.LINES]

    byte_labels = set()
    for annotator_id in si1.body.labels:
        for label in si1.body.labels[annotator_id]:
            assert OffsetType.BYTES in label.offsets
            byte_labels.add(label.target.target_id)

    line_labels = set()
    for annotator_id in si2.body.labels:
        for label in si2.body.labels[annotator_id]:
            assert OffsetType.LINES in label.offsets
            line_labels.add(label.target.target_id)

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
         ## will fail if set to bytes
         'offset_types': ['LINES']}
        )(stream_item)

    
