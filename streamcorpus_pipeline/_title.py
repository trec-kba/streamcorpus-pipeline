#!/usr/bin/env python

'''
streamcorpus_pipeline incremental transform for extracting a document
title and putting it in :attr:`~streamcorpus.StreamItem.other_content`

This software is released under an MIT/X11 open source license.

Copyright 2012-2014 Diffeo, Inc.
'''
from __future__ import absolute_import
import logging
import regex as re

from streamcorpus import ContentItem
from streamcorpus_pipeline.stages import Configured

logger = logging.getLogger(__name__)

title_re = re.compile(ur'(\n|.)*?\<title\>(?P<title>((\n|.)*?))\</title\>', re.IGNORECASE | re.UNICODE)
first_non_blank_re = re.compile(ur'(\n|.)*?(?P<title>((\n|.){,200}))', re.UNICODE)
whitespace_re = re.compile(ur'(\s|\n)+', re.UNICODE)


def add_content_item(stream_item, title_m):
    title = whitespace_re.sub(' ', title_m.group('title')).strip()
    if not isinstance(title, unicode):
        title = title.decode('utf-8')
    if len(title) > 60:
        title = title[:60] + '...'
    title = title.encode('utf-8')
    stream_item.other_content['title'] = ContentItem(clean_visible=title)


class title(Configured):
    '''Create "title" entry in
    :attr:`~streamcorpus.StreamItem.other_content`, if it does not
    already exist.
    '''
    config_name = 'title'
    default_config = {}

    def __init__(self, *args, **kwargs):
        super(title, self).__init__(*args, **kwargs)

    def __call__(self, stream_item, context=None):
        if not hasattr(stream_item, 'body') or not stream_item.body:
            logger.info('stream_item has no body')
            return stream_item
        if not stream_item.body.clean_visible:
            logger.info('stream_item.body has no clean_visible')
            return stream_item
        if 'title' in stream_item.other_content:
            oct = stream_item.other_content['title']
            if hasattr(oct, 'body') and hasattr(oct.body, 'clean_visible') \
               and oct.body.clean_visible:
                logger.info('stream_item.other_content already has "title": %r',
                            oct.body.clean_visible)
                return stream_item

        if stream_item.body.clean_html:
            chu = stream_item.body.clean_html.decode('utf-8')
            title_m = title_re.match(chu)
            if title_m:
                add_content_item(stream_item, title_m)
                return stream_item

        cvu = stream_item.body.clean_visible.decode('utf-8')
        title_m = first_non_blank_re.match(cvu)
        add_content_item(stream_item, title_m)
        return stream_item
