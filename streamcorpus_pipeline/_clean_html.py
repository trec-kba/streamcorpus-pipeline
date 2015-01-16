#!/usr/bin/env python
'''Convert raw text into cleaned HTML.

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2015 Diffeo, Inc.

.. autoclass:: clean_html

'''
from __future__ import absolute_import
import logging
import re
import string

import lxml.html
import lxml.html.clean
import lxml.html.soupparser
from BeautifulSoup import UnicodeDammit

from streamcorpus_pipeline.stages import Configured

logger = logging.getLogger(__name__)

encoding_re = re.compile(
    '''(?P<start_xml>([^<]|\n)*?\<\?xml[^>]*)'''
    '''(?P<encoding>encoding=.*?)(?P<remainder>(\s|\?\>)(.|\n)*)''',
    re.I)


def drop_invalid_and_upper_utf8_chars(possibly_invalid_string):
    '''Clean unexpected Unicode characters, including non-BMP.

    Returns a copy of `possibly_invalid_string` with the following
    replaced by space (U+0020): ASCII control characters other than
    tab, carriage return, and newline; reserved code points for
    surrogate pairs; invalid characters U+FFFE and U+FFFF; and
    supplementary characters U+10000 and higher.

    :param unicode possibly_invalid_string: string to clean
    :return: cleaned string
    :returntype: :class:`unicode`

    '''
    return re.sub(ur'[^\t\r\n\u0020-\ud7ff\ue000-\ufffd]', u' ',
                  possibly_invalid_string)


def force_unicode(raw):
    '''Try really really hard to get a Unicode copy of a string.

    First try :class:`BeautifulSoup.UnicodeDammit` to try to force
    to Unicode; if that fails, assume UTF-8 encoding, and ignore
    all errors.

    :param str raw: string to coerce
    :return: Unicode approximation of `raw`
    :returntype: :class:`unicode`

    '''
    converted = UnicodeDammit(raw, isHTML=True)
    if not converted.unicode:
        converted.unicode = unicode(raw, 'utf8', errors='ignore')

    encoding_m = encoding_re.match(converted.unicode)
    if encoding_m:
        converted.unicode = \
            encoding_m.group('start_xml') + \
            encoding_m.group('remainder')

    return converted.unicode


def make_clean_html(raw, stream_item=None):
    '''Get a clean text representation of presumed HTML.

    Treat `raw` as though it is HTML, even if we have no idea what it
    really is, and attempt to get a properly formatted HTML document
    with all HTML-escaped characters converted to their unicode.

    :param str raw: raw text to clean up
    :param stream_item: optional stream item with encoding metadata
    :type stream_item: :class:`streamcorpus.StreamItem`
    :returns: UTF-8-encoded byte string of cleaned HTML text
    :returntype: :class:`str`

    '''
    if stream_item and stream_item.body and stream_item.body.encoding:
        # if we know an encoding, then attempt to use it
        try:
            raw_decoded = raw.decode(stream_item.body.encoding)
        except:
            raw_decoded = raw
    else:
        raw_decoded = raw

    # default attempt uses vanilla lxml.html
    try:
        root = lxml.html.document_fromstring(raw_decoded)
    except ValueError, exc:
        if 'with encoding declaration' in str(exc):
            root = lxml.html.document_fromstring(raw)
        else:
            raise
    # if that worked, then we will be able to generate a
    # valid HTML string
    fixed_html = lxml.html.tostring(root, encoding=unicode)

    # remove any ^M characters
    fixed_html = string.replace(fixed_html, '\r', ' ')

    # We drop utf8 characters that are above 0xFFFF as
    # Lingpipe seems to be doing the wrong thing with them.
    fixed_html = drop_invalid_and_upper_utf8_chars(fixed_html)

    # construct a Cleaner that removes any ``<script>`` tags,
    # Javascript, like an ``onclick`` attribute, comments, style
    # tags or attributes, ``<link>`` tags
    cleaner = lxml.html.clean.Cleaner(
        scripts=True, javascript=True,
        comments=True,
        # do not remove <html> <head> <title> etc
        page_structure=False,
        style=True, links=True)

    # now get the really sanitized HTML
    _clean_html = cleaner.clean_html(fixed_html)

    # generate pretty HTML in utf-8
    _clean_html = lxml.html.tostring(
        lxml.html.document_fromstring(_clean_html),
        method='html', encoding='utf-8',
        pretty_print=True,
        # include_meta_content_type=True
        )

    return _clean_html


class clean_html(Configured):
    '''Create `body.clean_html` from `body.raw`.

    Configuration options:

    .. code-block:: yaml

        require_language_code: en

    If set, only work on stream items whose language code is the
    specified value; pass others on unchanged.  Defaults to unset.

    .. code-block:: yaml

        include_language_codes: [en, ""]

    If set to a non-empty list, only work on stream items whose
    language code is one of the specified values; pass others on
    unchanged.  Defaults to empty list (process all languages).

    '''
    config_name = 'clean_html'
    default_config = {'include_language_codes': []}

    def __init__(self, *args, **kwargs):
        super(clean_html, self).__init__(*args, **kwargs)
        self.require_code = self.config.get('require_language_code')
        self.codes = self.config.get('include_language_codes', [])

    def __call__(self, stream_item, context):
        if not stream_item.body or not stream_item.body.raw:
            logger.warn('dropping stream_item %s with missing body',
                        stream_item.stream_id)
            return None

        if (((not stream_item.body.language) and
             (self.require_code or self.codes))):
            logger.warn('skipping stream_item %s with missing language '
                        '(did you run the "language" transform?)',
                        stream_item.stream_id)
            return stream_item

        if ((self.require_code and
             self.require_code != stream_item.body.language.code)):
            logger.debug('skipping stream_item %s with language %r, '
                         'not required language %r',
                         stream_item.stream_id,
                         stream_item.body.language.code,
                         self.require_code)
            return stream_item

        if self.codes and stream_item.body.language.code not in self.codes:
            logger.debug('skipping stream_item %s with language %r, '
                         'not one of included codes %r',
                         stream_item.stream_id,
                         stream_item.body.language.code,
                         self.codes)

        if ((stream_item.body.media_type and
             stream_item.body.media_type.startswith('text/html'))):
            stream_item.body.clean_html = make_clean_html(
                stream_item.body.raw,
                stream_item=stream_item)
        else:
            logger.debug('skipping stream_item %s with non-HTML media_type %r',
                         stream_item.stream_id, stream_item.body.media_type)

        return stream_item
