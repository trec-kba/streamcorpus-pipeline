from __future__ import absolute_import, division, print_function

from streamcorpus import InvalidXpath, OffsetType, XpathRange
from streamcorpus.test_xpath import tests_roundtrip
import streamcorpus_pipeline.offsets as offsets
import streamcorpus_pipeline.tests._test_data as testdata


def test_simple_si(test_data_dir):
    si = testdata.get_si_simple_tagged_by_basis(test_data_dir)
    run_stream_item_roundtrip(si)


def test_random_chinese_si(test_data_dir):
    si = testdata.get_si_random_chinese_tagged_by_basis(test_data_dir)
    run_stream_item_roundtrip(si)


def test_wikipedia_chinese_si(test_data_dir):
    si = testdata.get_si_wikipedia_chinese_tagged_by_basis(test_data_dir)
    run_stream_item_roundtrip(si)


def run_stream_item_roundtrip(si):
    def print_window(token):
        coffset = token.offsets[OffsetType.CHARS]
        start = max(0, coffset.first - 100)
        end = min(len(html), coffset.first + coffset.length + 100)
        print(coffset)
        print(html[start:end])

    def debug(token, xprange, expected, err=None, got=None):
        print('-' * 79)
        if err is not None:
            print(err)
        print(xprange)
        print('expected: "%s"' % expected)
        if got is not None:
            print('got: "%s"' % got)
        print('token value: "%s"' % unicode(token.token, 'utf-8'))
        print('-' * 49)
        print_window(token)
        print('-' * 79)

    offsets.add_xpaths_to_stream_item(si)
    html = unicode(si.body.clean_html, 'utf-8')
    html_root = XpathRange.html_node(html)
    for sentences in si.body.sentences.itervalues():
        for sentence in sentences:
            for token in sentence.tokens:
                if OffsetType.CHARS not in token.offsets:
                    continue
                offset = token.offsets.get(OffsetType.XPATH_CHARS)
                if offset is None:
                    continue
                xprange = XpathRange.from_offset(offset)
                expected = unicode(token.token, 'utf-8')
                try:
                    got = xprange.slice_node(html_root)
                except InvalidXpath as err:
                    debug(token, xprange, expected, err=err)
                    assert False
                except NotImplementedError as err:
                    debug(token, xprange, expected, err=err)
                    assert False

                if expected != got:
                    debug(token, xprange, expected, got=got)
                assert expected == got, \
                    '%r, %r' % (xprange, token.offsets[OffsetType.CHARS])


# The code below takes a bunch of test specifications from `streamcorpus`
# and dynamically creates a function for each test case. The code is weird,
# but the end result is that we get a distinct test for each specification,
# which makes debugging easier. ---AG

for i, test in enumerate(tests_roundtrip):
    globals()['test_roundtrip_%d' % i] = (lambda t: lambda: run_test(t))(test)


def run_test(t):
    xpaths = list(offsets.char_offsets_to_xpaths(t['html'], t['tokens']))
    for token, xprange, expected in zip(t['tokens'], xpaths, t['expected']):
        if xprange is None:
            assert expected is None
        else:
            valid_html = '<html><body>' + t['html'] + '</body></html>'
            xprange = xprange.root_at('/html/body')
            got = xprange.slice_html(valid_html)
            assert expected == got, '%r\nHTML: %s' % (xprange, valid_html)
