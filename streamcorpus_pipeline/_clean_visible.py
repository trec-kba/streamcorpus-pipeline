'''Simple HTML to plain text conversion.

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2015 Diffeo, Inc.

.. autoclass:: clean_visible

'''
from __future__ import absolute_import
import logging
import re
import string
import traceback

import lxml.etree

from streamcorpus_pipeline._clean_html import drop_invalid_and_upper_utf8_chars
from streamcorpus_pipeline.stages import Configured
import yakonfig

logger = logging.getLogger(__name__)

# regex to identify all XML-like tags, including SCRIPT and STYLE tags
invisible = re.compile(
    # capture the visible text before the invisible part
    '''(?P<before>(.|\n)*?)'''
    # capture everything between SCRIPT and STYLE tags,
    '''(?P<invisible>(<script(.|\n)*?</script>|<style(.|\n)*?</style>'''
    # and also everything inside of XML-like tags, even if it
    # contains newlines
    '''|<(.|\n)*?>))''',
    # ignore case
    re.I)


def re_based_make_clean_visible(html):
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

    This is regex based, which can occassionally just hang...
    '''
    text = ''
    for m in invisible.finditer(html):
        text += m.group('before')
        text += ' ' * len(m.group('invisible'))

    # text better be >= original
    assert len(html) >= len(text), '%d !>= %d' % (len(html), len(text))

    # capture any characters after the last tag... such as newlines
    tail = len(html) - len(text)
    text += html[-tail:]

    # now they must be equal
    assert len(html) == len(text), '%d != %d' % (len(html), len(text))

    return text


def make_clean_visible(html, tag_replacement_char=' '):
    '''
    Takes an HTML-like binary string as input and returns a binary
    string of the same length with all tags replaced by whitespace.

    This does not detect comments, style, script, link.  It also does
    do anything with HTML-escaped characters.  All of these are
    handled by the clean_html pre-cursor step.

    Pre-existing whitespace of any kind (newlines, tabs) is converted
    to single spaces ' ', which has the same byte length (and
    character length).

    This is a simple state machine iterator without regexes
    '''
    def non_tag_chars():
        n = 0
        while n < len(html):
            angle = html.find('<', n)
            if angle == -1:
                yield html[n:]
                n = len(html)
                break
            yield html[n:angle]
            n = angle

            while n < len(html):
                nl = html.find('\n', n)
                angle = html.find('>', n)
                if angle == -1:
                    yield ' ' * (len(html) - n)
                    n = len(html)
                    break
                elif nl == -1 or angle < nl:
                    yield ' ' * (angle + 1 - n)
                    n = angle + 1
                    break
                else:
                    yield ' ' * (nl - n) + '\n'
                    n = nl + 1
                    # do not break

    return ''.join(non_tag_chars())


class clean_visible(Configured):
    '''Create ``body.clean_visible`` from ``body.clean_html``.

    :class:`clean_visible` maintains byte position of all visible text
    in an HTML (or XML) document and removes all of parts that are not
    visible in a web browser.  This allows taggers to operate on the
    visible-only text and any standoff annotation can refer to the
    original byte positions.

    If there is no ``clean_html``, but there is a ``raw`` property
    with a ``text/plain`` media type, use that value directly.

    This has no useful configuration options.  The configuration
    metadata will include a setting:

    .. code-block:: yaml

        require_clean_html: true

    Setting this to ``false`` will always fail.

    '''
    config_name = 'clean_visible'
    default_config = {'require_clean_html': True}

    @staticmethod
    def check_config(config, name):
        if not config['require_clean_html']:
            raise yakonfig.ConfigurationError('{} only does clean_html'
                                              .format(name))

    def __call__(self, stream_item, context):
        if stream_item.body:
            if stream_item.body.clean_html:
                stream_item.body.clean_visible = \
                    make_clean_visible(stream_item.body.clean_html)
                logger.debug('stream item %s: '
                             'generated %d bytes of clean_visible '
                             'from %d bytes of clean_html',
                             stream_item.stream_id,
                             len(stream_item.body.clean_visible),
                             len(stream_item.body.clean_html))
            elif (stream_item.body.raw and
                  stream_item.body.media_type == 'text/plain'):
                stream_item.body.clean_visible = stream_item.body.raw
                logger.debug('stream item %s: '
                             'generated %d bytes of clean_visible '
                             'from text/plain raw content',
                             stream_item.stream_id,
                             len(stream_item.body.clean_visible))
            else:
                logger.debug('skipped stream item %s: '
                             'neither clean_html nor text/plain content',
                             stream_item.stream_id)
        return stream_item


def make_clean_visible_file(i_chunk, clean_visible_path):
    '''make a temp file of clean_visible text'''
    _clean = open(clean_visible_path, 'wb')
    _clean.write('<?xml version="1.0" encoding="UTF-8"?>')
    _clean.write('<root>')
    for idx, si in enumerate(i_chunk):
        if si.stream_id is None:
            # create the FILENAME element anyway, so the ordering
            # remains the same as the i_chunk and can be aligned.
            stream_id = ''
        else:
            stream_id = si.stream_id
        doc = lxml.etree.Element("FILENAME", stream_id=stream_id)
        if si.body and si.body.clean_visible:
            try:
                # is UTF-8, and etree wants .text to be unicode
                doc.text = si.body.clean_visible.decode('utf8')
            except ValueError:
                doc.text = drop_invalid_and_upper_utf8_chars(
                    si.body.clean_visible.decode('utf8'))
            except Exception, exc:
                # this should never ever fail, because if it does,
                # then it means that clean_visible (or more likely
                # clean_html) is not what it is supposed to be.
                # Therefore, do not take it lightly:
                logger.critical(traceback.format_exc(exc))
                logger.critical('failed on stream_id=%s to follow:',
                                si.stream_id)
                logger.critical(repr(si.body.clean_visible))
                logger.critical('above was stream_id=%s', si.stream_id)
                # [I don't know who calls this, but note that this
                # will *always* fail if clean_visible isn't valid UTF-8.]
                raise
        else:
            doc.text = ''
        _clean.write(lxml.etree.tostring(doc, encoding='UTF-8'))
    _clean.write('</root>')
    _clean.close()
    logger.info(clean_visible_path)

    '''
    ## hack to capture html for inspection
    _html = open(clean_visible_path + '-html', 'wb')
    for idx, si in enumerate(i_chunk):
        _html.write('<FILENAME docid="%s">' % si.stream_id)
        if si.body and si.body.clean_html:
            _html.write(si.body.clean_html)
        _html.write('</FILENAME>\n')
    _html.close()
    ## replace this with log.info()
    print clean_visible_path + '-html'
    '''

# used in 'cleanse' below
whitespace = re.compile('''(\s|\n)+''', re.UNICODE)
strip_punctuation = {ord(c): u' ' for c in string.punctuation}
penn_treebank_brackets = re.compile('''-[RL].B-''', re.UNICODE)


def cleanse(span, lower=True):
    '''Convert a unicode string into a lowercase string with no
punctuation and only spaces for whitespace.

Replace PennTreebank escaped brackets with ' ':
-LRB- -RRB- -RSB- -RSB- -LCB- -RCB-
(The acronyms stand for (Left|Right) (Round|Square|Curly) Bracket.)
http://www.cis.upenn.edu/~treebank/tokenization.html

:param span: string
    '''
    assert isinstance(span, unicode), \
        'got non-unicode string %r' % span
    # lowercase, strip punctuation, and shrink all whitespace
    span = penn_treebank_brackets.sub(' ', span)
    if lower:
        span = span.lower()
    span = span.translate(strip_punctuation)
    span = whitespace.sub(' ', span)
    # trim any leading or trailing whitespace
    return span.strip()
