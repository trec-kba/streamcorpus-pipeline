

from _hyperlink_labels import anchors_re

sample_text = u'''
This is a normal <a href="http://en.wikipedia.org/wiki/Hello">link</a>.

This is a funky <a
href ='http://en.wikipedia.org/wiki/Bogus'  asdf=4
>the Bogus</a>
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
        print m.groups('href')

    ## now we check more of the output
    assert len(matches) == 2

