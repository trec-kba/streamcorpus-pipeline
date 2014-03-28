#!/usr/bin/env python
'''
Pipeline "transform" for converting raw text into clean_html

This software is released under an MIT/X11 open source license.

Copyright 2012-2014 Diffeo, Inc.
'''
from __future__ import absolute_import
import logging
import os
import re
import string
import traceback

import lxml.html
import lxml.html.clean
import lxml.html.soupparser
from BeautifulSoup import UnicodeDammit

import streamcorpus
from streamcorpus_pipeline.stages import Configured
from streamcorpus_pipeline import _exceptions

logger = logging.getLogger(__name__)

encoding_re = re.compile(
    '''(?P<start_xml>([^<]|\n)*?\<\?xml[^>]*)''' + \
    '''(?P<encoding>encoding=.*?)(?P<remainder>(\s|\?\>)(.|\n)*)''',
    re.I)

def valid_XML_char_ordinal(ord_c):
    ## http://stackoverflow.com/questions/8733233/filtering-out-certain-bytes-in-python
    return ( # conditions ordered by presumed frequency
        0x20 <= ord_c <= 0xD7FF 
        or ord_c in (0x9, 0xA, 0xD)
        or 0xE000 <= ord_c <= 0xFFFD
        or 0x10000 <= ord_c <= 0x10FFFF
        )

def drop_invalid_XML_chars(possibly_invalid_string):
    return ''.join(
        c if valid_XML_char_ordinal(ord(c)) else ' '
        for c in possibly_invalid_string         
        )

def lower_utf8_char(ord_c):
    return (  
        0x20 <= ord_c <= 0xD7FF 
        or ord_c in (0x9, 0xA, 0xD)
        or 0xE000 <= ord_c <= 0xFFFD
        )

def drop_invalid_and_upper_utf8_chars(possibly_invalid_string):
    return ''.join(
        c if lower_utf8_char(ord(c)) else ' '
        for c in possibly_invalid_string         
        )

def make_traceback_log(all_exc):
    ## convert array of Exceptions into a string of tracebacks
    traceback_log = ''
    for num, exc in enumerate(all_exc):
        traceback_log += '\n\nexc #%d\n%s' % (num, traceback.format_exc(exc))
    return traceback_log

def make_clean_html_super(raw, stream_item=None, log_dir_path=None):
    '''
    Treat 'raw' as though it is HTML, even if we have no idea what it
    really is, and attempt to get a properly formatted HTML document
    with all HTML-escaped characters converted to their unicode.
    '''
    ## attempt to get HTML and force it to unicode
    fixed_html = None

    ## count the number of attempts, so can get progressively more
    ## aggressive with forcing the character set
    attempt = 0

    ## keep all the tracebacks, so we can read them if we want to
    ## analyze a particular document
    all_exc = []

    ## the last attempt leads sets this to True to end the looping
    no_more_attempts = False
    while not no_more_attempts:
        attempt += 1

        try:
            ## default attempt uses vanilla lxml.html
            root = lxml.html.fromstring(raw)
            ## if that worked, then we will be able to generate a
            ## valid HTML string
            fixed_html = lxml.html.tostring(root, encoding='unicode')

        except UnicodeDecodeError, exc:
            ## most common failure is a bogus encoding
            all_exc.append(exc)
            try:
                converted = UnicodeDammit(raw, isHTML=True)
                if not converted.unicode:
                    raise Exception(
                        'UnicodeDammit failed, appeared to be %r tried [%s]' % (
                            converted.originalEncoding,
                            ', '.join(converted.triedEncodings)))

                encoding_m = encoding_re.match(converted.unicode)
                if encoding_m:
                    converted.unicode = \
                        encoding_m.group('start_xml') + \
                        encoding_m.group('remainder')

                root = lxml.html.fromstring(converted.unicode)
                ## if that worked, then we will be able to generate a
                ## valid HTML string
                fixed_html = lxml.html.tostring(root, encoding='unicode')

                ## hack in a logging step here so we can manually inspect
                ## this fallback stage.
                if log_dir_path and stream_item:
                    stream_item.body.clean_html = fixed_html.encode('utf8')
                    stream_item.body.logs.append( make_traceback_log(all_exc) )

            except Exception, exc:
                ## UnicodeDammit failed
                all_exc.append(exc)
                fixed_html = None

        except Exception, exc:
            ## lxml.html failed with something other than UnicodeDecodeError
            all_exc.append(exc)
            fixed_html = None

        if fixed_html is None:
            try:
                ## can soupparser build a valid tree?
                root = lxml.html.soupparser.fromstring(raw)
                fixed_html = lxml.html.tostring(root, encoding='unicode')

                ## hack in a logging step here so we can manually inspect
                ## this fallback stage.
                if log_dir_path and stream_item:
                    stream_item.body.clean_html = fixed_html.encode('utf8')
                    stream_item.body.logs.append( make_traceback_log(all_exc) )

            except Exception, exc:
                ## soupparser failed
                all_exc.append(exc)
                fixed_html = None

        if fixed_html is not None:
            ## success!  Stop looping and process fixed_html
            break

        elif attempt == 1:
            ## try stripping control characters from the beginning
            initial_100 = raw[:100]
            fixed_initial_100 = drop_invalid_XML_chars(initial_100)
            raw = fixed_initial_100 + raw[100:]
            ## now it will loop through again

        elif attempt == 2:
            ## try stripping control characters from entire text
            raw = drop_invalid_XML_chars(raw)
            ## now it will loop through again

        elif attempt == 3:
            ## force latin1, which often is often the right thing to
            ## do anyway, because someone set utf8 as a default header
            ## when they meant latin1
            raw = raw.decode('latin-1', 'replace')

        else:
            no_more_attempts = True

    ## out of the loop, if we didn't get fixed_html, then raise all
    ## the traceback strings together.
    if not fixed_html:
        raise Exception(make_traceback_log(all_exc))

    ## construct a Cleaner that removes any ``<script>`` tags,
    ## Javascript, like an ``onclick`` attribute, comments, style
    ## tags or attributes, ``<link>`` tags
    cleaner = lxml.html.clean.Cleaner(
        scripts=True, javascript=True, 
        comments=True, 
        style=True, links=True)

    ## now get the really sanitized HTML
    _clean_html = cleaner.clean_html(fixed_html)

    ## generate pretty HTML in utf-8
    _clean_html = lxml.html.tostring(
        lxml.html.document_fromstring(_clean_html), 
        method='html', encoding='utf-8',
        pretty_print=True, 
        #include_meta_content_type=True
        )

    ## remove any ^M characters
    _clean_html = string.replace( _clean_html, '\r', '' )

    return _clean_html

def force_unicode(raw):
    '''
    Uses BeautifulSoup.UnicodeDammit to try to force to unicode, and
    if that fails, it assumes utf8 and just ignores all errors.
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

def make_clean_html(raw, stream_item=None, log_dir_path=None):
    '''
    Treat 'raw' as though it is HTML, even if we have no idea what it
    really is, and attempt to get a properly formatted HTML document
    with all HTML-escaped characters converted to their unicode.
    '''
    #logger.debug('repr(raw):')
    #logger.debug(repr(raw))
    if stream_item and stream_item.body and stream_item.body.encoding:
        ## if we know an encoding, then attempt to use it
        try:
            raw_decoded = raw.decode(stream_item.body.encoding)
        except:
            raw_decoded = raw
    else:
        raw_decoded = raw

    try:
        ## default attempt uses vanilla lxml.html
        try:
            root = lxml.html.document_fromstring(raw_decoded)
        except ValueError, exc:
            if 'with encoding declaration' in str(exc):
                root = lxml.html.document_fromstring(raw)
            else:
                raise exc
        ## if that worked, then we will be able to generate a
        ## valid HTML string
        fixed_html = lxml.html.tostring(root, encoding=unicode)
    except (TypeError, lxml.etree.ParserError, UnicodeDecodeError), exc:
        logger.critical( '_clean_html.make_clean_html caught %s' % exc )
        raise _exceptions.TransformGivingUp()

    #logger.debug( 'repr(fixed_html):' )
    #logger.debug( repr(fixed_html) )

    ## remove any ^M characters
    fixed_html = string.replace( fixed_html, '\r', ' ' )

    ## remove any invalid chars, such as those resulting from this
    ## bogus HTML-escaped entity &#822050+ that reulted from someone
    ## failing to put the ";" between "&#8220;" and "50+"

    #logger.debug( 'first' )
    #logger.debug( fixed_html.encode('utf8') )

    ## We drop utf8 characters that are above 0xFFFF as 
    ## Lingpipe seems to be doing the wrong thing with them. 
    fixed_html = drop_invalid_and_upper_utf8_chars(fixed_html)

    #logger.debug( 'second' )
    #logger.debug( fixed_html.encode('utf8') )

    ## construct a Cleaner that removes any ``<script>`` tags,
    ## Javascript, like an ``onclick`` attribute, comments, style
    ## tags or attributes, ``<link>`` tags
    cleaner = lxml.html.clean.Cleaner(
        scripts=True, javascript=True, 
        comments=True, 
        ## do not remove <html> <head> <title> etc
        page_structure=False,
        style=True, links=True)

    ## now get the really sanitized HTML
    _clean_html = cleaner.clean_html(fixed_html)

    #logger.debug( 'third' )
    #logger.debug( _clean_html.encode('utf8') )

    ## generate pretty HTML in utf-8
    _clean_html = lxml.html.tostring(
        lxml.html.document_fromstring(_clean_html), 
        method='html', encoding='utf-8',
        pretty_print=True, 
        #include_meta_content_type=True
        )

    #logger.debug( 'fourth' )
    #logger.debug( _clean_html )

    return _clean_html

class clean_html(Configured):
    '''Create body.clean_html from body.raw.

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

        log_dir_path: yes

    If set, record any Unicode decoding errors and other exceptions in
    body.logs on the stream item.  Note that this looks like a "path"
    variable and will be rewritten to an absolute path, so its value
    must be a string, but it only matters that the value is non-empty;
    the actual value is unused.  Defaults to unset.

    '''
    config_name = 'clean_html'
    default_config = {'include_language_codes': [] }

    def __init__(self, *args, **kwargs):
        super(clean_html, self).__init__(*args, **kwargs)
        self.require_code = self.config.get('require_language_code')
        self.codes = self.config.get('include_language_codes', [])
        self.log_dir_path = self.config.get('log_dir_path', None)

    def __call__(self, stream_item, context):
        if self.require_code:
            ## need to check stream_item for language
            if not stream_item.body.language or \
                    self.require_code != stream_item.body.language.code:
                ## either missing or different
                return stream_item

        elif self.codes:
            if stream_item.body.language and \
                    stream_item.body.language.code not in self.codes:
                ## has code and not the right one
                return stream_item

        if stream_item.body and stream_item.body.raw \
                and stream_item.body.media_type == 'text/html':

            logger.debug('making clean html for %s %r' % (
                    stream_item.stream_id, stream_item.body.language))

            stream_item.body.clean_html = make_clean_html(
                stream_item.body.raw, 
                stream_item=stream_item,
                log_dir_path=self.log_dir_path)

        return stream_item

