from __future__ import absolute_import, division, print_function

from streamcorpus.test_xpath import tests_roundtrip
import streamcorpus_pipeline.offsets as offsets
import streamcorpus_pipeline.tests._test_data as testdata


def test_simple_si(test_data_dir):
    si = testdata.get_si_simple_tagged_by_basis(test_data_dir)
    offsets.add_xpaths_to_stream_item(si)
    offsets.stream_item_roundtrip_xpaths(si)


def test_random_chinese_si(test_data_dir):
    si = testdata.get_si_random_chinese_tagged_by_basis(test_data_dir)
    offsets.add_xpaths_to_stream_item(si)
    offsets.stream_item_roundtrip_xpaths(si)


def test_wikipedia_chinese_si(test_data_dir):
    si = testdata.get_si_wikipedia_chinese_tagged_by_basis(test_data_dir)
    offsets.add_xpaths_to_stream_item(si)
    offsets.stream_item_roundtrip_xpaths(si)


def test_irish_duo_si(test_data_dir):
    si = testdata.get_si_irish_duo_tagged_by_basis(test_data_dir)
    offsets.add_xpaths_to_stream_item(si)
    offsets.stream_item_roundtrip_xpaths(si)


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
