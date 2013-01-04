#!/usr/bin/env python
'''
Pipeline "transform" for converting raw text into clean_html

This software is released under an MIT/X11 open source license.

Copyright 2012 Diffeo, Inc.
'''
import re
import os
import string
import traceback
import streamcorpus
import lxml.html
import lxml.html.clean
import lxml.html.soupparser
from BeautifulSoup import UnicodeDammit

if __name__ != '__main__':
    from ._logging import log_full_file

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

def drop_invalid_XML_char(possibly_invalid_string):
    return ''.join(
        c for c in possibly_invalid_string 
        if valid_XML_char_ordinal(ord(c))
        )

def make_traceback_log(all_exc):
    ## convert array of Exceptions into a string of tracebacks
    traceback_log = ''
    for num, exc in enumerate(all_exc):
        traceback_log += '\n\nexc #%d\n%s' % (num, traceback.format_exc(exc))
    return traceback_log

def make_clean_html(raw, stream_item=None, log_dir=None):
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
                if log_dir and stream_item:
                    stream_item.body.clean_html = fixed_html.encode('utf8')
                    stream_item.body.logs.append( make_traceback_log(all_exc) )
                    log_full_file(stream_item, 'fallback-UnicodeDammit', log_dir)

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
                if log_dir and stream_item:
                    stream_item.body.clean_html = fixed_html.encode('utf8')
                    stream_item.body.logs.append( make_traceback_log(all_exc) )
                    log_full_file(stream_item, 'fallback-soupparser', log_dir)
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
            fixed_initial_100 = drop_invalid_XML_char(initial_100)
            raw = fixed_initial_100 + raw[100:]
            ## now it will loop through again

        elif attempt == 2:
            ## try stripping control characters from entire text
            raw = drop_invalid_XML_char(raw)
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

def clean_html(config):
    '''
    returns a kba.pipeline "transform" function that attempts to
    generate stream_item.body.clean_html from body.raw
    '''
    ## make a closure around config
    def _make_clean_html(stream_item):
        if stream_item.body and stream_item.body.raw \
                and stream_item.body.media_type == 'text/html':

            stream_item.body.clean_html = make_clean_html(
                stream_item.body.raw, 
                stream_item=stream_item,
                log_dir=config['clean_html']['log_dir'])

        return stream_item

    return _make_clean_html

if __name__ == '__main__':
    open('nytimes-index-clean.html', 'wb').write(
        make_clean_html(open('nytimes-index.html').read()))
