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

from streamcorpus import Token, Sentence, EntityType, Chunk, Offset, \
    OffsetType, Gender, MentionType, Attribute, AttributeType
from streamcorpus_pipeline.stages import Configured
from streamcorpus_pipeline._taggers import TaggerBatchTransform

logger = logging.getLogger(__name__)

## map LingPipe's specific strings to streamcorpus.EntityType
_ENTITY_TYPES = {
    'PERSON': EntityType.PER,
    'FEMALE_PRONOUN': EntityType.PER,
    'MALE_PRONOUN': EntityType.PER,
    'ORGANIZATION': EntityType.ORG,
    'LOCATION': EntityType.LOC,
    }

_PRONOUNS = {
    'FEMALE_PRONOUN': Gender.FEMALE,
    'MALE_PRONOUN': Gender.MALE,
    }

filename_re = re.compile('''.*?<FILENAME docid="(?P<stream_id>.*?)">(?P<tagged_doc>(.|\n)*?)</FILENAME>''')
sentence_re = re.compile('''(?P<before>(.|\n)*?)<s i="(?P<sentence_id>.*?)">(?P<tagged_sentence>(.|\n)*?)</s>''')
ner_re = re.compile('''(?P<before>(.|\n)*?)(<ENAMEX ID="(?P<chain_id>.*?)" TYPE="(?P<entity_type>.*?)">(?P<entity_string>(.|\n)*?)</ENAMEX>)?''')

## detect any kind of whitespace, including unicode whitespace, which
## should include \u200b, even though python2.7 apparently lost this
## knowledge:  http://bugs.python.org/issue10567
only_whitespace = re.compile(u'''^(\s|\n|\u200b)*$''', re.UNICODE)

def files(text):
    '''
    Iterate over <FILENAME> XML-like tags and tokenize with nltk
    '''
    for f_match in filename_re.finditer(text):
        yield f_match.group('stream_id'), f_match.group('tagged_doc')

class LingPipeParser(object):
    def __init__(self, config):
        self.config = config
        self.clear()

    def clear(self):
        self.tok_num = 0
        self.byte_idx = 0
        self.line_idx = 0
        self.word_tokenizer = WhitespaceTokenizer()

    def set(self, ner_dom):
        self.clear()
        ## nltk wants a unicode string, so decode, it and then we will
        ## re-encode it to carefully recover the byte offsets.  We
        ## must take care not to use any nltk components that insert
        ## new whitespace, such
        ## nltk.tokenize.treebank.TreebankTokenizer
        self.ner_dom = ner_dom
        self.attributes = []
        self.relations = []

    def sentences(self):
        '''
        Iterate over <s> XML-like tags and tokenize with nltk
        '''
        for sentence_id, node in enumerate(self.ner_dom.childNodes):
            ## increment the char index with any text before the <s>
            ## tag.  Crucial assumption here is that the LingPipe XML
            ## tags are inserted into the original byte array without
            ## modifying the portions that are not inside the
            ## LingPipe-added tags themselves.
            if node.nodeType == node.TEXT_NODE:
                ## we expect to only see TEXT_NODE instances with whitespace
                assert only_whitespace.match(node.data), repr(node.data)

                ## must convert back to utf-8 to have expected byte offsets
                self.byte_idx += len(node.data.encode('utf-8'))

                ## count full lines, i.e. only those that end with a \n
                # 'True' here means keep the trailing newlines
                for line in node.data.splitlines(True):
                    if line.endswith('\n'):
                        self.line_idx += 1
            else:
                logger.debug('getting tokens for sentence_id=%d' % sentence_id)
                more_sentence_remains = True
                while more_sentence_remains:
                    ## always a sentence
                    sent = Sentence()

                    ## this "node" came from for loop above, and it's
                    ## childNodes list might have been popped by a
                    ## previous pass through this while loop
                    tokens = iter( self.tokens( node ) )

                    while 1:
                        try:
                            tok = tokens.next()
                            sent.tokens.append(tok)
                            #logger.debug('got token: %r  %d %d' % (tok.token, tok.mention_id, tok.sentence_pos))

                        except StopIteration:
                            yield sent
                            more_sentence_remains = False
                            break

    def _make_token(self, start, end):
        '''
        Instantiates a Token from self._input_string[start:end]
        '''
        ## all thfift strings must be encoded first
        tok_string = self._input_string[start:end].encode('utf-8')
        if only_whitespace.match(tok_string):
            ## drop any tokens with only whitespace
            return None
        tok = Token()
        tok.token = tok_string
        tok.token_num = self.tok_num
        if 'BYTES' in self.config['offset_types']:
            tok.offsets[OffsetType.BYTES] = Offset(
                type =  OffsetType.BYTES,
                first=self.byte_idx + len(self._input_string[:start].encode('utf-8')),
                length=len(tok_string),
                value=self.config['offset_debugging'] and tok_string or None,
                )
        if 'LINES' in self.config['offset_types']:
            tok.offsets[OffsetType.LINES] = Offset(
                type =  OffsetType.LINES,
                first=self.line_idx,
                length=1,
                value=self.config['offset_debugging'] and tok_string or None,
                )
        self.tok_num += 1
        ## keep track of position within a sentence
        tok.sentence_pos = self.sent_pos
        self.sent_pos += 1
        return tok

    def tokens(self, sentence_dom):
        '''
        Tokenize all the words and preserve NER labels from ENAMEX tags
        '''
        ## keep track of sentence position, which is reset for each
        ## sentence, and used above in _make_token
        self.sent_pos = 0
    
        ## keep track of mention_id, so we can distinguish adjacent
        ## multi-token mentions within the same coref chain
        mention_id = 0

        while len(sentence_dom.childNodes) > 0:
            ## shrink the sentence_dom's child nodes.  In v0_2_0 this
            ## was required to cope with HitMaxi16.  Now it is just to
            ## save memory.
            node = sentence_dom.childNodes.pop(0)

            if node.nodeType == node.TEXT_NODE:
                ## process portion before an ENAMEX tag
                for line in node.data.splitlines(True):
                    self._input_string = line
                    for start, end in self.word_tokenizer.span_tokenize(line):
                        tok = self._make_token(start, end)
                        if tok:
                            yield tok

                    if line.endswith('\n'):
                        ## maintain the index to the current line
                        self.line_idx += 1

                    ## increment index pasat the 'before' portion
                    self.byte_idx += len(line.encode('utf-8'))

            else:
                ## process text inside an ENAMEX tag
                assert node.nodeName == 'ENAMEX', node.nodeName
                chain_id = node.attributes.get('ID').value
                entity_type = node.attributes.get('TYPE').value
                for node in node.childNodes:
                    assert node.nodeType == node.TEXT_NODE, node.nodeType
                    for line in node.data.splitlines(True):
                        self._input_string = line
                        for start, end in self.word_tokenizer.span_tokenize(line):
                            tok = self._make_token(start, end)
                            if tok:
                                if entity_type in _PRONOUNS:
                                    tok.mention_type = MentionType.PRO
                                    tok.entity_type = _ENTITY_TYPES[entity_type]
                                    
                                    ## create an attribute
                                    attr = Attribute(
                                        attribute_type=AttributeType.PER_GENDER,
                                        value=str(_PRONOUNS[entity_type])
                                        )
                                    self.attributes.append(attr)

                                else:
                                    ## regular entity_type
                                    tok.mention_type = MentionType.NAME
                                    tok.entity_type = _ENTITY_TYPES[entity_type]

                                tok.equiv_id = int(chain_id)
                                tok.mention_id = mention_id
                                yield tok

                        if line.endswith('\n'):
                            ## maintain the index to the current line
                            self.line_idx += 1

                        ## increment index pasat the 'before' portion
                        self.byte_idx += len(line.encode('utf-8'))

                ## increment mention_id within this sentence
                mention_id += 1


class lingpipe(TaggerBatchTransform):
    '''
    a streamcorpus_pipeline batch transform that converts a chunk into a new
    chunk with data generated by LingPipe
    '''
    config_name = 'lingpipe'
    tagger_id = 'lingpipe'

    template = \
        '''cd %(tagger_root_path)s/demos/generic/bin && ''' + \
        '''cat %(clean_visible_path)s | ./cmd_coref_en_news_muc6.sh ''' + \
        ''' "-contentType=text/html" "-includeElts=FILENAME" ''' + \
        ''' %(java_heap_size)s 1> %(ner_xml_path)s'''

    def get_sentences(self, ner_dom):
        '''parse the sentences and tokens out of the XML'''
        lp_parser = LingPipeParser(self.config)
        lp_parser.set(ner_dom)
        sentences = list( lp_parser.sentences() )
        return sentences, lp_parser.relations, lp_parser.attributes


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('action', 
                        metavar='postproc|align', 
                        help='postproc')
    parser.add_argument('input_file', help='XML file from LingPipe')
    parser.add_argument('output_file', help='XML file to generate with OWPL data')
    parser.add_argument('--source_chunk', help='source chunk file that was input to the pipeline data')
    args = parser.parse_args()

    if args.action == 'postproc':
        text = open(args.input_file).read()
        print 'read %d bytes from %s' % (len(text), args.input_file)

        raise NotImplementedError('need to instantiate a LingPipeParser object here')
        
        for stream_id, tagged_doc in files(text):
            for sent in sentences(tagged_doc):  # pylint: disable=E0602
                for tok in sent.tokens:
                    if tok.entity_type is not None:
                        print tok, EntityType._VALUES_TO_NAMES[tok.entity_type]

    elif args.action == 'align':
        i_chunk = Chunk(path=args.source_chunk, mode='rb')
        o_chunk = Chunk(path=args.output_file,  mode='wb')
        align_chunk_with_ner(args.input_file, i_chunk, o_chunk)  # pylint: disable=E0602
