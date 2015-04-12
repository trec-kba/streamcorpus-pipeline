from __future__ import absolute_import
import logging
import os
import time
import uuid

import pytest

import streamcorpus
from streamcorpus import make_stream_item, StreamItem, ContentItem, OffsetType, Chunk
import streamcorpus_pipeline
from streamcorpus_pipeline._clean_visible import clean_visible
from streamcorpus_pipeline._hyperlink_labels import anchors_re, hyperlink_labels
from streamcorpus_pipeline.tests._test_data import get_test_chunk_path

logger = logging.getLogger(__name__)

def make_test_stream_item(test_data_dir):
    stream_item = make_stream_item(None, 'http://nytimes.com/')
    stream_item.body = ContentItem()
    path = os.path.join(test_data_dir, 'test', 'nytimes-index-clean-stable.html')
    stream_item.body.clean_html = open(str(path)).read()
    return stream_item

def make_hyperlink_labeled_test_stream_item(test_data_dir):
    context = {}
    si = make_test_stream_item(test_data_dir)
    assert len(si.body.clean_html) > 200
    hl = hyperlink_labels(config={
        'require_abs_url': True,
        'all_domains': True,
        'offset_types': ['BYTES'],
    })
    hl(si, context)
    cv = clean_visible(config={})
    cv(si, context)
    assert len(si.body.clean_visible) > 200
    return si

def make_hyperlink_labeled_test_chunk(tmpdir):
    '''
    returns a path to a temporary chunk that has been hyperlink labeled
    '''
    tpath = tmpdir.join(str(uuid.uuid1()) + '.sc')
    o_chunk = Chunk(tpath, mode='wb')

    ipath = get_test_chunk_path()

    hl = hyperlink_labels(config={
        'require_abs_url': True,
        'all_domains': True,
        'offset_types': [BYTES],
    })
    cv = make_clean_visible(config={})
    for si in Chunk(path=ipath, message=streamcorpus.StreamItem_v0_2_0):
        ## clear out existing labels and tokens
        si.body.labels = {}
        si.body.sentences = {}
        context = {}
        hl(si, context)
        cv(si, context)
        o_chunk.add(si)

        o_chunk.close()
        return tpath

def test_basics(test_data_dir):
    start = time.time()
    ## run it with a byte regex
    si1 = make_test_stream_item(test_data_dir)
    context = {}
    hl1 = hyperlink_labels(config={
        'require_abs_url': True,
        'all_domains': False,
        'domain_substrings': ['nytimes.com'],
        'offset_types': ['BYTES'],
    })
    hl1(si1,context)
    elapsed_bytes = time.time() - start

    assert si1.body.labels['author'][0].offsets.keys() == [OffsetType.BYTES]

    ## run it with regex
    start = time.time()
    si2 = make_test_stream_item(test_data_dir)
    hl2 = hyperlink_labels(config={
        'require_abs_url': True,
        'all_domains': False,
        'domain_substrings': ['nytimes.com'],
        'offset_types': ['LINES'],
    })
    hl2(si2,context)
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
    logger.info('{0:.5f} bytes, {1:.5f} lines'
                .format(elapsed_bytes, elapsed_lines))



@pytest.mark.parametrize(('parser_type',), [  # pylint: disable=E1101
    ('BYTES',),
    ('CHARS',),
])
def test_speed(parser_type, test_data_dir):
    stream_items = []
    for i in xrange(10):
        stream_item = StreamItem()
        stream_item.body = ContentItem()
        path = os.path.join(test_data_dir, 'test' )
        stream_item.body.clean_html = open(
            os.path.join(path, 'nytimes-index-clean.html')).read()
        stream_items.append( stream_item )

    context = {}
    start = time.time()
    hl = hyperlink_labels(config={
        'require_abs_url': True,
        'all_domains': False,
        'domain_substrings': ['nytimes.com'],
        'offset_types': [parser_type],
    })
    for si in stream_items:
        si = hl(si, context)
        elapsed = time.time() - start

    rate = len(stream_items) / elapsed

    logger.debug('OffsetType: {0}'.format(OffsetType))
    logger.info('{0:.1f} per second for {1}'.format(rate, parser_type))


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
        # logger.debug(m.groups('href')

    assert sum(map(int, map(bool, matches))) == 3


@pytest.mark.parametrize(('parser_type',), [  # pylint: disable=E1101
    ('BYTES',),
    ('CHARS',),
])
def test_long_doc(parser_type, test_data_dir):
    stream_item = StreamItem()
    stream_item.body = ContentItem()
    path = os.path.join(test_data_dir, 'test' )
    stream_item.body.clean_html = open(
        os.path.join(path, 'company-test.html')).read()

    context = {}
    hl = hyperlink_labels(config={
        'require_abs_url': True,
        'all_domains': True,
        'offset_types': [parser_type],
    })
    hl(stream_item, context)
