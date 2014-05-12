#!/usr/bin/env python
'''
streamcorpus_pipeline incremental transform for extracting a document
title and putting it in :attr:`~streamcorpus.StreamItem.other_content`

This software is released under an MIT/X11 open source license.

Copyright 2012-2014 Diffeo, Inc.
'''
from __future__ import absolute_import
import logging
import re

from streamcorpus import ContentItem
from streamcorpus_pipeline.stages import Configured
from streamcorpus_pipeline import _exceptions

logger = logging.getLogger(__name__)

title_re = re.compile('(\n|.)*?\<title\>(?P<title>((\n|.)*?))\</title\>', re.I)
first_non_blank_re = re.compile('(\n|.)*?(?P<title>((\n|.){,200}))')
whitespace_re = re.compile('(\s|\n)+')

def add_content_item(stream_item, title_m):
    title = whitespace_re.sub(' ', title_m.group('title')).strip()
    if len(title) > 60:
        title = title[:60] + '...'
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
            logger.info('stream_item.other_content has no "title"')
            return stream_item

        if stream_item.body.clean_html:
            title_m = title_re.match(stream_item.body.clean_html)
            if title_m:
                add_content_item(stream_item, title_m)
                return stream_item

        title_m = first_non_blank_re.match(stream_item.body.clean_visible)
        add_content_item(stream_item, title_m)
        return stream_item
