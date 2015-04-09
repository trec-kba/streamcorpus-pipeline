from __future__ import absolute_import, division, print_function

from streamcorpus_pipeline.offsets import tokens_to_xpaths


tests_roundtrip = [{
    'html': '<p>Foo</p>',
    'tokens': [(3, 6)],
}, {
    'html': '<p>Foo</p><p>Bar</p>',
    'tokens': [(3, 6), (13, 16)],
}, {
    'html': '<p><a>Foo</a></p>',
    'tokens': [(6, 9)],
}, {
    'html': '<p>Homer <b>Jay</b> Simpson</p>',
    'tokens': [(3, 8), (12, 15), (20, 27)],
}, {
    'html': '<b><i>a<u>b</u>c</i>d</b>',
    'tokens': [(6, 7), (10, 11), (15, 16), (20, 21)],
}, {
    'html': '<b>AB</b>',
    'tokens': [(3, 4), (4, 5)],
}, {
    'html': '<b>ABCD</b>',
    'tokens': [(3, 5), (5, 7)],
}, {
    'html': '<b>b<i>a</i>r</b>',
    'tokens': [(3, 13)],
    'expected': ['bar'],
}, {
    'html': '<b>Hello, Homer <i>Jay</i> Simpson. Press any key.</b>',
    'tokens': [(10, 34)],
    'expected': ['Homer Jay Simpson'],
}, {
    'html': '<b>b<i>a</i>r</b>',
    'tokens': [(3, 4), (7, 8), (12, 13)],
# Getting these tests to pass will require a general tree traversal. ---AG
# }, {
    # 'html': '<b>b<i>ar</i></b>',
    # 'tokens': [(3, 8)],
    # 'expected': ['bar'],
# }, {
    # 'html': '<b>f<i>o</i>ob<u>a</u>r</b>',
    # 'tokens': [(3, 23)],
}]


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
