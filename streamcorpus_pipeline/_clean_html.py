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

import html5lib
import lxml.etree
import lxml.html
import lxml.html.clean
import lxml.html.soupparser
from BeautifulSoup import UnicodeDammit

from streamcorpus_pipeline.emails import fix_emails
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


def nice_decode(raw, stream_item=None, encoding=None):
    raw_decoded = None
    if encoding is not None:
        try:
            raw_decoded = raw.decode(encoding)
        except:
            logger.warn('encoding=%r provided, but failed',
                        encoding, exc_info=True)
            raw_decoded = None

    if raw_decoded is None and stream_item is not None:
        if stream_item.body and stream_item.body.encoding:
            encoding = stream_item.body.encoding
            try:
                raw_decoded = raw.decode(encoding)
            except:
                logger.warn('stream_item.body.encoding=%r provided, but failed',
                            encoding, exc_info=True)
                raw_decoded = None

        if raw_decoded and stream_item.body and stream_item.body.media_type:
            #text/html; charset=windows-1251
            parts = stream_item.body.media_type.split('=')
            if len(parts) >= 2:
                encoding = parts[1].strip()
                try:
                    raw_decoded = raw.decode(encoding)
                except:
                    logger.warn('stream_item.body.content_type=%r provided, but failed',
                                stream_item.body.content_type, exc_info=True)
                    raw_decoded = None

    return raw_decoded


def make_clean_html(raw, stream_item=None, encoding=None):
    '''Get a clean text representation of presumed HTML.

    Treat `raw` as though it is HTML, even if we have no idea what it
    really is, and attempt to get a properly formatted HTML document
    with all HTML-escaped characters converted to their unicode.

    This is called below by the `clean_html` transform stage, which
    interprets MIME-type.  If `character_encoding` is not provided,
    and `stream_item` is provided, then this falles back to
    :attr:`streamcorpus.StreamItem.body.encoding`.

    :param str raw: raw text to clean up
    :param stream_item: optional stream item with encoding metadata
    :type stream_item: :class:`streamcorpus.StreamItem`
    :returns: UTF-8-encoded byte string of cleaned HTML text
    :returntype: :class:`str`

    '''
    # Fix emails by protecting the <,> from HTML
    raw = fix_emails(raw)
    raw_decoded = nice_decode(raw, stream_item=stream_item, encoding=encoding)
    if raw_decoded is None:
        # give up on decoding it... maybe this should use force_unicode
        raw_decoded = raw

    # default attempt uses vanilla lxml.html
    try:
        root = lxml.html.document_fromstring(raw_decoded)
    except ValueError, exc:
        if 'with encoding declaration' in str(exc):
            root = lxml.html.document_fromstring(raw)
        else:
            raise

    # While we have the document parsed as a DOM, let's strip attributes.
    # (The HTML cleaner seems to only support whitelisting attributes.
    # As of now, we just want to blacklist a few.)
    lxml.etree.strip_attributes(root, 'class', 'id')

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
        remove_tags=['base'],
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

    return uniform_html(_clean_html)


def uniform_html(html):
    '''Takes a utf-8-encoded string of HTML as input and returns a new
    HTML string with fixed quoting and close tags, which generally
    should not break any of the offsets and makes it easier for
    functions like
    :func:`streamcorpus_pipeline.offsets.char_offsets_to_xpaths` to
    operate without failures.

    '''
    doc = html5lib.parse(html.decode('utf-8'))
    config = {
        'omit_optional_tags': False,
        'encoding': 'utf-8',
        'quote_attr_values': True,
    }
    return html5lib.serializer.serialize(doc, **config)


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

    .. code-block:: yaml

        include_mime_types: ['text/html']

    If set to a non-empty list, only work on stream items whose
    mime type matches one of the specified values. If set to an
    empty list, then all stream items (assuming they match the
    above language criteria) will have ``clean_html`` populated
    from ``body.raw``. Default to list of just ``text/html``.

    '''
    config_name = 'clean_html'
    default_config = {
        'include_language_codes': [],
        'include_mime_types': ['text/html'],
    }

    def __init__(self, *args, **kwargs):
        super(clean_html, self).__init__(*args, **kwargs)
        self.require_code = self.config.get('require_language_code')
        self.codes = self.config.get('include_language_codes', [])
        self.include_mime_types = \
            [s.lower()
             for s in self.config.get('include_mime_types', ['text/html'])]

    def is_matching_mime_type(self, mime_type):
        '''This implements the MIME-type matching logic for deciding whether
        to run `make_clean_html`

        '''
        if len(self.include_mime_types) == 0:
            return True
        if mime_type is None:
            return False
        mime_type = mime_type.lower()
        # NB: startswith is necessary here, because encodings are
        # often appended to HTTP header Content-Type
        return any(mime_type.startswith(mt) for mt in self.include_mime_types)

    def __call__(self, stream_item, context):
        if not stream_item.body or not stream_item.body.raw:
            logger.warn('dropping stream_item %s with missing body',
                        stream_item.stream_id)
            return None

        lang = stream_item.body.language
        if not lang and (self.require_code or self.codes):
            logger.warn('skipping stream_item %s with missing language '
                        '(did you run the "language" transform?)',
                        stream_item.stream_id)
            return stream_item

        if self.require_code and self.require_code != lang.code:
            logger.info('skipping stream_item %s with language %r, '
                        'not required language %r',
                        stream_item.stream_id,
                        stream_item.body.language.code,
                        self.require_code)
            return stream_item

        if self.codes and lang.code not in self.codes:
            logger.info('skipping stream_item %s with language %r, '
                        'not one of included codes %r',
                        stream_item.stream_id, lang.code, self.codes)

        if self.is_matching_mime_type(stream_item.body.media_type):
            stream_item.body.clean_html = make_clean_html(
                stream_item.body.raw,
                stream_item=stream_item)
        else:
            logger.info('skipping stream_item %s with unrecognized '
                        'media_type %r (allowed mime types: %r)',
                        stream_item.stream_id, stream_item.body.media_type,
                        self.include_mime_types or 'EVERYTHING')
        return stream_item
