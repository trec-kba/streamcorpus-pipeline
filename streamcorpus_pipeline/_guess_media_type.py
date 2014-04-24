#!/usr/bin/env python
'''
streamcorpus_pipeline Transform for gathering stats about file types.

This software is released under an MIT/X11 open source license.

Copyright 2012-2014 Diffeo, Inc.
'''
from __future__ import absolute_import
import re

from streamcorpus_pipeline.stages import Configured

first_letters = re.compile('''^(.|\n)*?(?P<first_letters>\S+\s?\S+)''')

## ignore up to 20 non '<' characters including line feeds looking for
## doctype XXXX, where the most common is XXX=HTML
#doctype_something_m = re.compile('''^([^<]|\r|\n){,20}\<\!doctype\s+(?P<doctype>\S+)''', re.I)
doctype_re = re.compile(
    ## skip up to 100 null bytes or whitespaces
    '''(?P<chunk>(\x00|\s|\r|\n)*?)''' + \
    ## allow an optional XML preamble
    '''(?P<xml_preamble>(\<\?xml([^>]|\n)*?\>(\s|\r|\n){,100})?)''' + \
    ## capture the doctype
    '''\<\!doctype\s+(?P<doctype>[a-zA-Z0-9]{,10})''',
    re.I)

probably_html = re.compile(
    ## skip up to twenty bytes of stuff and up to 1000 whitespaces
    ## from start of file
    #'''^(?P<chunk>([^<]|\r|\n){,20}(\s|\r|\n){,1000})''' + \
    '''[^<]*''' + \
    ## allow an optional XML preamble
    '''(?P<xml_preamble>(\<\?xml([^>]|\n)*?\>(\s|\r|\n){,1000})?)''' + \
    ## allow an optional DOCTYPE preamble without a doctype
    '''(\<\!doctype\s*\>(\s|\r|\n){,1000})?''' + \
    ## look for a first tag that is very likely to HTML
    '''(?P<first_tag>\<(html|head|frame|body|iframe|title|table|br|style|base\s+href|script|meta|link|div|\!--))''', 
    re.I)

xml_ish = re.compile('''^([^<]|\r|\n){,20}(?P<intro>\<\?xml[^>]*?\>(.|\n){50})''', re.I)

pdf_start = re.compile('''^(\s|\r|\n){,20}\%pdf-(?P<version>.{5})''', re.I)

#import sys

tags = 'html|head|frame|body|iframe|title|table|br|style|base\s+href|script|meta|link|div'
def has_tags(text):
    text = text.lower()
    for tag in tags.split('|'):
        if tag in text:
            return True
    return False

def file_type_stats(config):
    '''
    returns a kba.pipeline "transform" function that generates file
    type stats from the stream_items that it sees.  Currently, these
    stats are just the first five non-whitespace characters.
    '''
    ## make a closure around config
    def _file_type_stats(stream_item, context):
        if stream_item.body and stream_item.body.raw:
            #print repr(stream_item.body.raw[:250])
            #sys.stdout.flush()
            #doctype_m = doctype_re.match(stream_item.body.raw[:250])
            #if doctype_m:
                #print 'DOCTYPE: %s' % repr(doctype_m.group('doctype').lower())
            if 'doctype html' in stream_item.body.raw[:250].lower():
                print 'DOCTYPE: html'
            else:
                #if probably_html.search(stream_item.body.raw):
                if has_tags(stream_item.body.raw[:400]):
                    print 'PROBABLY_HTML'
                else:
                    xml = xml_ish.search(stream_item.body.raw)
                    if xml:
                        print 'XML: %s' % repr(xml.group('intro'))
                    else:
                        pdf = pdf_start.search(stream_item.body.raw)
                        if pdf:
                            print 'PDF %s' % repr(pdf.group('version'))
                        else:
                            ext = stream_item.abs_url.split('.')[-1]
                            if len(ext) < 6:
                                print 'UNK ext: %s' % repr(ext)
                            else:
                                first = first_letters.match(stream_item.body.raw)
                                if first and False:
                                    print 'UNK letters: %s' % repr(first.group('first_letters'))
                                else:
                                    print 'UNK first bytes: %s' % repr(stream_item.body.raw[:50])
                    #m = first_three_letters.search(stream_item.body.raw)
                    #if m:
                    #    print repr(m.group('first_three_letters')).lower().strip()
                    #else:
                    #    print repr(stream_item.body.raw[:50]).lower().strip()
        return stream_item

    return _file_type_stats


class guess_media_type(Configured):
    '''
    returns a kba.pipeline "transform" function that populates
    body.media_type if it is empty and the content type is easily
    guessed.
    '''
    config_name = 'guess_media_type'
    def __call__(self, stream_item, context):
        if stream_item.body and stream_item.body.media_type:
            ## don't change it
            return stream_item

        if stream_item.body and stream_item.body.raw:
            if len(stream_item.body.raw) < 40:
                stream_item.body.raw = None
                stream_item.body.media_type = None

            elif 'doctype html' in stream_item.body.raw[:250].lower():
                stream_item.body.media_type = 'text/html'

            elif has_tags( stream_item.body.raw[:400] ):
                stream_item.body.media_type = 'text/html'

            elif stream_item.body.raw[:10].lower().startswith('%pdf-'):
                stream_item.body.media_type = 'application/pdf'

        if stream_item.body and not stream_item.body.media_type \
                and self.config.get('fallback_media_type'):
            stream_item.body.media_type = self.config['fallback_media_type']

        return stream_item
