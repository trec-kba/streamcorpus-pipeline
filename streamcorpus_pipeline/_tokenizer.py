#!/usr/bin/env python
'''
streamcorpus_pipeline.TaggerBatchTransform for LingPipe

This software is released under an MIT/X11 open source license.

Copyright 2012-2014 Diffeo, Inc.
'''
from __future__ import absolute_import
import hashlib
import itertools
import logging
import os
import re
import sys
import time
import traceback
import uuid

from nltk.tokenize import WhitespaceTokenizer
from nltk.tokenize.punkt import PunktWordTokenizer, PunktSentenceTokenizer

from sortedcollection import SortedCollection
import streamcorpus
from streamcorpus import Token, Sentence, EntityType, Chunk, Offset, OffsetType, Gender, MentionType, Attribute, AttributeType
from streamcorpus_pipeline.stages import IncrementalTransform
from streamcorpus_pipeline._taggers import TaggerBatchTransform

logger = logging.getLogger(__name__)

class nltk_tokenizer(IncrementalTransform):
    '''
    a streamcorpus_pipeline IncrementalTransform that converts a chunk into a new
    chunk with Sentence objects generated using NLTK tokenizers
    '''
    config_name = 'nltk_tokenizer'
    tagger_id = 'nltk_tokenizer'
    def __init__(self, *args, **kwargs):
        super(nltk_tokenizer, self).__init__(*args, **kwargs)
        self.sentence_tokenizer = PunktSentenceTokenizer()
        self.word_tokenizer = WhitespaceTokenizer() #PunktWordTokenizer()

    def _sentences(self, clean_visible):
        'generate strings identified as sentences'
        previous_end = 0
        clean_visible = clean_visible.decode('utf8')
        assert isinstance(clean_visible, unicode)
        for start, end in self.sentence_tokenizer.span_tokenize(clean_visible):
            ## no need to check start, because the first byte of text
            ## is always first byte of first sentence, and we will
            ## have already made the previous sentence longer on the
            ## end if there was an overlap.
            if  start < previous_end:
                start = previous_end
                if start > end:
                    ## skip this sentence... because it was eaten by
                    ## an earlier sentence with a label
                    continue
            try:
                label = self.label_index.find_le(end)
            except ValueError:
                label = None
            if label:
                off = label.offsets[OffsetType.BYTES]
                end = max(off.first + off.length, end)
            previous_end = end
            sent_str = clean_visible[start:end]
            yield start, end, sent_str

    def make_label_index(self, stream_item):
        'make a sortedcollection on body.labels'
        labels = stream_item.body.labels.get(self.config.get('annotator_id'))
        if not labels:
            labels = []

        self.label_index = SortedCollection(
            labels,
            key=lambda label: label.offsets[OffsetType.BYTES].first)

    def make_sentences(self, stream_item):
        'assemble Sentence and Token objects'
        self.make_label_index(stream_item)
        sentences = []
        token_num = 0
        new_mention_id = 0
        for sent_start, sent_end, sent_str in self._sentences(stream_item.body.clean_visible):
            assert isinstance(sent_str, unicode)
            sent = Sentence()
            sentence_pos = 0
            for start, end in self.word_tokenizer.span_tokenize(sent_str):
                token_str = sent_str[start:end].encode('utf8')
                tok = Token(
                    token_num=token_num,
                    token=token_str,
                    sentence_pos=sentence_pos,
                )
                tok.offsets[OffsetType.BYTES] = Offset(
                    type=OffsetType.BYTES, 
                    first=sent_start + start,
                    length = end - start,
                )
                ## whitespace tokenizer will never get a token
                ## boundary in the middle of an 'author' label
                try:
                    #logger.debug('searching for %d in %r', sent_start + start, self.label_index._keys)
                    label = self.label_index.find_le(sent_start + start)
                except ValueError:
                    label = None
                if label:
                    off = label.offsets[OffsetType.BYTES]
                    if off.first + off.length > sent_start + start:
                        logger.info('overlapping label: %r' % label.target.target_id)
                        ## overlaps
                        streamcorpus.add_annotation(tok, label)
                        assert label.annotator.annotator_id in tok.labels

                        logger.info('adding label to tok: %r has %r',
                                     tok.token, label.target.target_id)

                        if label in self.label_to_mention_id:
                            mention_id = self.label_to_mention_id[label]
                        else:
                            mention_id = new_mention_id
                            new_mention_id += 1
                            self.label_to_mention_id[label] = mention_id

                        tok.mention_id = mention_id

                token_num += 1
                sentence_pos += 1
                sent.tokens.append(tok)
            sentences.append(sent)
        return sentences

    def process_item(self, stream_item, context=None):
        if not hasattr(stream_item.body, 'clean_visible') or not stream_item.body.clean_visible:
            return stream_item
            
        self.label_index = None
        self.label_to_mention_id = dict()
        stream_item.body.sentences[self.tagger_id] = self.make_sentences(stream_item)

        return stream_item

    def __call__(self, stream_item, context=None):
        ## support the legacy callable API
        return self.process_item(stream_item, context)
        
if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('input')
    args = parser.parse_args()

    logger = logging.getLogger('streamcorpus_pipeline')
    ch = logging.StreamHandler()
    logger.addHandler(ch)
    streamcorpus_logger = logging.getLogger('streamcorpus')
    streamcorpus_logger.addHandler(ch)

    t = nltk_tokenizer(dict(annotator_id='author'))
    for si in Chunk(args.input):
        t.process_item(si)
        if si.body.clean_visible:
            assert len(si.body.sentences['nltk_tokenizer']) > 0
            logger.critical( 'num_sentences=%d, has wikipedia %r and %d labels',
                             len(si.body.sentences['nltk_tokenizer']),  
                             'wikipedia.org' in si.body.clean_html,
                             len(getattr(si.body, 'labels', {}).get('author', {})) )

            #if len(getattr(si.body, 'labels', {}).get('author', {})) > 3:
            #    c = Chunk('foo.sc', mode='wb')
            #    c.add(si)
            #    c.close()
            #    sys.exit()
        
