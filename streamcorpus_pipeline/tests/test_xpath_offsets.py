from __future__ import absolute_import, division, print_function

from streamcorpus.test_xpath import tests_roundtrip
from streamcorpus_pipeline.offsets import tokens_to_xpaths


def run_strict_test(test):
    xpaths = tokens_to_xpaths(test['html'], test['tokens'])
    assert test['xpaths'] == xpaths
    run_test(test)


def run_test(test):
    xpaths = list(tokens_to_xpaths(test['html'], test['tokens']))
    expecteds = test.get('expected', [None] * len(xpaths))
    for token, xprange, expected in zip(test['tokens'], xpaths, expecteds):
        if expected is None:
            expected = test['html'][token[0]:token[1]]
        valid_html = '<html><body>' + test['html'] + '</body></html>'
        xprange = xprange.root_at('/html/body')
        got = xprange.from_html(valid_html)
        assert expected == got, '%r\nHTML: %s' % (xprange, valid_html)


for i, test in enumerate(tests_roundtrip):
    globals()['test_roundtrip_%d' % i] = (lambda t: lambda: run_test(t))(test)
