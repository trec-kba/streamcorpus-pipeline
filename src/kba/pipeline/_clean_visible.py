#!/usr/bin/env python
'''
clean_visible maintains byte position of all visible text in an HTML (or
XML) document and removes all of parts that are not visible in a web
browser.  This allows taggers to operate on the visible-only text and
any standoff annotation can refer to the original byte positions.

This software is released under an MIT/X11 open source license.

Copyright 2012 Diffeo, Inc.
'''

import re
## regex to identify all XML-like tags, including SCRIPT and STYLE tags
invisible = re.compile(
    ## capture the visible text before the invisible part
    '''(?P<before>(.|\n)*?)''' + \
    ## capture everything between SCRIPT and STYLE tags,
    '''(?P<invisible>(<script(.|\n)*?</script>|<style(.|\n)*?</style>''' + \
    ## and also everything inside of XML-like tags, even if it
    ## contains newlines
    '''|<(.|\n)*?>))''',
    ## ignore case
    re.I)

def make_clean_visible(html):
    '''
    Takes an HTML-like binary string as input and returns a binary
    string of the same length with all tags replaced by whitespace.
    This also detects script and style tags, and replaces the text
    between them with whitespace.  

    Pre-existing whitespace of any kind (newlines, tabs) is converted
    to single spaces ' ', which has the same byte length (and
    character length).

    Note: this does not change any characters like &rsquo; and &nbsp;,
    so taggers operating on this text must cope with such symbols.
    Converting them to some other character would change their byte
    length, even if equivalent from a character perspective.
    '''
    text = ''
    for m in invisible.finditer(html):
        text += m.group('before')
        text += ' ' * len(m.group('invisible'))

    ## text better be >= original
    assert len(html) >= len(text), '%d !>= %d' % (len(html), len(text))

    ## capture any characters after the last tag... such as newlines
    tail = len(html) - len(text)
    text += html[-tail:]
    
    ## now they must be equal
    assert len(html) == len(text), '%d != %d' % (len(html), len(text))

    return text

def clean_visible(config):
    '''
    returns a kba.pipeline "transform" function that attempts to
    generate stream_item.body.clean_visible from body.clean_html
    '''
    ## make a closure around config
    def _make_clean_visible(stream_item):
        if stream_item.body and stream_item.body.clean_html:
            stream_item.body.clean_visible = \
                make_clean_visible(stream_item.body.clean_html)
        return stream_item

    return _make_clean_visible
