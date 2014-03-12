#!/usr/bin/env python
'''
kba.pipeline Transform for converting v0_1_0 StreamItems to v0_2_0

This software is released under an MIT/X11 open source license.

Copyright 2012-2014 Diffeo, Inc.
'''
from __future__ import absolute_import
import logging

from streamcorpus import make_stream_item, make_stream_time, ContentItem, Tagging, Rating, Target, Annotator
from streamcorpus_pipeline.stages import Configured

logger = logging.getLogger(__name__)

class keep_annotated(Configured):
    '''
    returns a kba.pipeline "transform" function that populates takes
    in v0_1_0 StreamItems and emits v0_2_0 StreamItems
    '''
    config_name = 'keep_annotated'
    def __init__(self, config):
        super(keep_annotated, self).__init()
        fh = open(self.config['annotation_file'])
        annotated_stream_ids = dict()
        for line in fh.readlines():
            if line.startswith('#'):
                continue
            parts = line.split()
            stream_id = parts[2]
            if stream_id not in annotated_stream_ids:
                annotated_stream_ids[ stream_id ] = Rating(annotator=Annotator(annotator_id=parts[0]),
                                                           target=Target(target_id='http://en.wikipedia.org/wiki/%s' % parts[3]))

    def __call__(self, s1, context):
        if s1.stream_id in annotated_stream_ids:
            r = annotated_stream_ids[s1.stream_id]
            s1.ratings[r.annotator.annotator_id] = [r]
            return s1
        else:
            return None

class upgrade_streamcorpus(Configured):
    '''
    returns a kba.pipeline "transform" function that populates takes
    in v0_1_0 StreamItems and emits v0_2_0 StreamItems
    '''
    config_name = 'upgrade_streamcorpus'
    default_config = { 'keep_old_cleansed_as_clean_visible': False }
    def __call__(self, s1, context):
        s2 = make_stream_item(s1.stream_time.zulu_timestamp,
                              s1.abs_url)
        s2.schost = s1.schost
        s2.source = s1.source
        s2.source_metadata['kba-2012'] = s1.source_metadata

        logger.debug('len(original .body.raw) = %d' % len( s1.body.raw ))

        #logger.critical(repr(s2))

        s2.body = ContentItem(
            raw = s1.body.raw,
            encoding = s1.body.encoding,
            ## default, might get overwritten below
            media_type = 'text/html',
            taggings = {'stanford': Tagging(
                    tagger_id = 'stanford',
                    raw_tagging = s1.body.ner,
                    generation_time = make_stream_time('2012-06-01T00:00:00.0Z'),
                    tagger_config = 'annotators: {tokenize, cleanxml, ssplit, pos, lemma, ner}, properties: pos.maxlen=100',
                    tagger_version = 'Stanford CoreNLP ver 1.2.0',
                    )}
            )

        if self.config['keep_old_cleansed_as_clean_visible']:
            s2.body.clean_visible = s1.body.cleansed

        if s1.source == 'social':
            s2.body.media_type = 'text/plain'
            ## the separation of content items in the social stream
            ## was artificial and annoying, so smoosh them together
            s2.body.clean_visible = '\n\n'.join([
                    s1.title.cleansed,
                    s1.anchor.cleansed,
                    s1.body.cleansed])

            changed_body_raw = False
            if s1.title and s1.title.raw:
                s2.body.raw = s1.title.raw
                s2.body.raw += r'\n\n'
                changed_body_raw = True

            if s1.anchor and s1.anchor.raw:
                s2.body.raw += s1.anchor.raw
                s2.body.raw += r'\n\n'
                changed_body_raw = True

            if changed_body_raw:
                s2.body.raw += s1.body.raw

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
