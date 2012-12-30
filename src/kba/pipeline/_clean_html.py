#!/usr/bin/env python
'''
Pipeline "transform" for converting raw text into clean_html

This software is released under an MIT/X11 open source license.

Copyright 2012 Diffeo, Inc.
'''

import os
import traceback
import streamcorpus
import lxml.html
import lxml.html.clean
import lxml.html.soupparser
from BeautifulSoup import UnicodeDammit

def make_clean_html(raw):
    '''
    Treat 'raw' as though it is HTML, even if we have no idea what it
    really is, and attempt to get a properly formatted HTML document
    encoded in UTF-8 with all of the non-visible content stripped.
    '''
    ## attempt to get HTML and force it to unicode
    try:
        root = lxml.html.fromstring(raw)
        ## if that worked, then we will be able to generate a
        ## valid HTML string
        fixed_html = lxml.html.tostring(root, encoding='unicode')

    except UnicodeDecodeError:
        try:
            converted = UnicodeDammit(raw, isHTML=True)
            if not converted.unicode:
                raise Exception(
                    'UnicodeDammit failed, appeared to be %r tried [%s]' % (
                        converted.originalEncoding,
                        ', '.join(converted.triedEncodings)))

            root = lxml.html.fromstring(converted.unicode)
            ## if that worked, then we will be able to generate a
            ## valid HTML string
            fixed_html = lxml.html.tostring(root, encoding='unicode')

        except Exception, exc:
            print('Simple converstions failed: %s' % traceback.format_exc(exc))

            ## Otherwise, try having the soupparser build a valid tree
            ## out of the soup
            root = lxml.html.soupparser.fromstring(raw)
            ## use the same function to get HTML in UTF-8
            fixed_html = lxml.html.tostring(root, encoding='unicode')


    ## construct a Cleaner that removes any ``<script>`` tags,
    ## Javascript, like an ``onclick`` attribute, comments, style
    ## tags or attributes, ``<link>`` tags
    cleaner = lxml.html.clean.Cleaner(
        scripts=True, javascript=True, 
        comments=True, 
        style=True, links=True)

    ## now get the really sanitized HTML
    _clean_html = cleaner.clean_html(fixed_html)

    ## generate pretty HTML in UTF-8
    _clean_html = lxml.html.tostring(
        lxml.html.document_fromstring(_clean_html), 
        method='html', encoding='utf-8',
        pretty_print=True, 
        include_meta_content_type=True
        )

    return _clean_html

def clean_html(stream_item):
    '''
    a kba.pipeline "transform" function that generates clean_html when
    possible
    '''
    if stream_item.body and stream_item.body.raw:
        try:
            _clean_html = make_clean_html(stream_item.body.raw)
            stream_item.body.clean_html = _clean_html
        except Exception, exc:
            ## logger?
            pass

    return stream_item
