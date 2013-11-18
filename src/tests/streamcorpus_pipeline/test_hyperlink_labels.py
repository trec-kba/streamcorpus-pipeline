import os
import time
import uuid
import pytest 
from streamcorpus import make_stream_item, StreamItem, ContentItem, OffsetType, Chunk
from streamcorpus_pipeline._hyperlink_labels import anchors_re, hyperlink_labels
from streamcorpus_pipeline.stages import _init_stage
from streamcorpus_pipeline._logging import logger
from tests.streamcorpus_pipeline._test_data import _TEST_DATA_ROOT

def make_test_stream_item():
    stream_item = make_stream_item(None, 'http://nytimes.com/')
    stream_item.body = ContentItem()
    path = os.path.dirname(__file__)
    path = os.path.join( path, _TEST_DATA_ROOT, 'test', 'nytimes-index-clean.html')
    stream_item.body.clean_html = open(path).read()
    return stream_item

def make_hyperlink_labeled_test_stream_item():
    context = {}
    si = make_test_stream_item()
    hyperlink_labels(
        {'require_abs_url': True, 
         'all_domains': True,
         'offset_types': ['BYTES']}
        )(si, context)

    cv = _init_stage('clean_visible', {})
    cv(si, context)

    return si
    
def make_hyperlink_labeled_test_chunk():
    '''
    returns a path to a temporary chunk that has been hyperlink labeled
    '''
    tpath = os.path.join('/tmp', str(uuid.uuid1()) + '.sc')
    o_chunk = Chunk(tpath, mode='wb')

    dpath = os.path.dirname(__file__)
    ipath = os.path.join( dpath, _TEST_DATA_ROOT, 'test/WEBLOG-100-fd5f05c8a680faa2bf8c55413e949bbf.sc' )

    cv = _init_stage('clean_visible', {})
    hl = hyperlink_labels(
        {'require_abs_url': True, 
         'all_domains': True,
         'offset_types': ['BYTES']}
        )
    for si in Chunk(path=ipath):
        ## clear out existing labels and tokens
        si.body.labels = {}
        si.body.sentences = {}
        context = {}
        hl(si, context)
        cv(si, context)
        o_chunk.add(si)

    o_chunk.close()
    return tpath
    
def test_basics():
    start = time.time()
    ## run it with a byte regex
    si1 = make_test_stream_item()
    context = {}
    hyperlink_labels(
        {'require_abs_url': True, 
         'domain_substrings': ['nytimes.com'],
         'all_domains': False,
         'offset_types': ['BYTES']}
        )(si1, context)
    elapsed_bytes = time.time() - start

    assert si1.body.labels['author'][0].offsets.keys() == [OffsetType.BYTES]

    start = time.time()
    si2 = make_test_stream_item()
    ## run it with regex
    hyperlink_labels(
        {'require_abs_url': True, 
         'domain_substrings': ['nytimes.com'],
         'all_domains': False,
         'offset_types': ['LINES']}
        )(si2, context)
    elapsed_lines = time.time() - start

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
    print '\n\n%.5f bytes,\n %.5f lines' % (elapsed_bytes, elapsed_lines)



@pytest.mark.parametrize(('parser_type',), [  # pylint: disable=E1101
    ('BYTES',),
    ('LINES',),
])
def test_speed(parser_type):
    stream_items = []
    for i in xrange(10):
        stream_item = StreamItem()
        stream_item.body = ContentItem()
        path = os.path.dirname(__file__)
        path = os.path.join( path, _TEST_DATA_ROOT, 'test' )
        stream_item.body.clean_html = open(
            os.path.join(path, 'nytimes-index-clean.html')).read()
        stream_items.append( stream_item )

    context = {}
    start = time.time()
    ## run it with a byte state machine
    for si in stream_items:
        si = hyperlink_labels(
            {'require_abs_url': True, 
             'domain_substrings': ['nytimes.com'],
             'all_domains': False,
             'offset_types': [parser_type]}
            )(si, context)
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
    parts = sample_text.split('</a>')
    matches = list(anchors_re.match(part) for part in parts)

    ## now we check more of the output
    assert len(matches) == 4

    for m in matches:
        if not m:
            continue
        before = m.group('before')
        href = m.group('href')
        ahref = m.group('ahref')
        posthref = m.group('posthref')
        preequals = m.group('preequals')
        postequals = m.group('postequals')
        #print m.groups('href')

    assert sum(map(int, map(bool, matches))) == 3


@pytest.mark.parametrize(('parser_type',), [  # pylint: disable=E1101
    ('BYTES',),
    ('LINES',),
])
def test_long_doc(parser_type):
    stream_item = StreamItem()
    stream_item.body = ContentItem()
    path = os.path.dirname(__file__)
    path = os.path.join( path, _TEST_DATA_ROOT, 'test' )
    stream_item.body.clean_html = open(
        os.path.join(path, 'company-test.html')).read()

    context = {}
    ## run it with a byte state machine
    hyperlink_labels(
        {'require_abs_url': True, 
         'all_domains': True,
         ## will fail if set to bytes
         'offset_types': [parser_type]}
        )(stream_item, context)

    
