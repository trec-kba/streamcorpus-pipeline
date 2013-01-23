#!/usr/bin/env python
'''
kba.pipeline Transform for converting v0_1_0 StreamItems to v0_2_0

This software is released under an MIT/X11 open source license.

Copyright 2012 Diffeo, Inc.
'''

from streamcorpus import make_stream_item, ContentItem

def upgrade_streamcorpus(config):
    '''
    returns a kba.pipeline "transform" function that populates takes
    in v0_1_0 StreamItems and emits v0_2_0 StreamItems
    '''
    ## make a closure around config
    def _upgrade_streamcorpus(s1):
        s2 = make_stream_item(s1.stream_time.zulu_timestamp,
                              s1.abs_url)
        s2.schost = s1.schost
        s2.source = s1.source
        s2.source_metadata['kba-2012'] = s1.source_metadata
        s2.body = ContentItem(
            raw = s1.body.raw,
            encoding = s1.body.encoding,
            ## default, might get overwritten below
            media_type = 'text/html',
            )
        if s1.source == 'social':
            s2.body.media_type = 'text/plain'
            ## the separation of content items in the social stream
            ## was artificial and annoying, so smoosh them together
            s2.body.clean_visible = '\n\n'.join([
                    s1.title.cleansed,
                    s1.anchor.cleansed,
                    s1.body.cleansed])
        if s1.title:
            ci = ContentItem(
                raw = s1.title.raw,
                encoding = s1.title.encoding,
                clean_visible = s1.title.cleansed,
                )
            s2.other_content['title'] = ci
        if s1.anchor:
            ci = ContentItem(
                raw = s1.anchor.raw,
                encoding = s1.anchor.encoding,
                clean_visible = s1.anchor.cleansed
                )
            s2.other_content['anchor'] = ci
        return s2

    return _upgrade_streamcorpus
