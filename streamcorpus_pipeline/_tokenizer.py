'''Pipeline transform for :mod:`nltk`.

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2015 Diffeo, Inc.

.. autoclass:: nltk_tokenizer

'''
from __future__ import absolute_import
import logging

from nltk.tokenize import WhitespaceTokenizer
from nltk.tokenize.punkt import PunktSentenceTokenizer

from sortedcollection import SortedCollection
import streamcorpus
from streamcorpus import Token, Sentence, Offset, OffsetType
from streamcorpus_pipeline.stages import IncrementalTransform

logger = logging.getLogger(__name__)


class nltk_tokenizer(IncrementalTransform):
    '''Minimal tokenizer using :mod:`nltk`.

    This is an incremental transform that creates minimal tokens and
    does minimal label alignment.  The stream item must have a
    `clean_visible` part already.  The stream item is tagged with
    ``nltk_tokenizer``.

    This tokenizer has very few dependencies, only the :mod:`nltk`
    Python package.  However, the tokens that it creates are
    extremely minimal, with no part-of-speech details or other
    information.  The tokens only contain the `token` property and
    character-offset information.

    If there are labels with character offsets attached to the stream
    item body, and this stage is configured with an `annotator_id`,
    then this stage attempts to attach those labels to tokens.

    '''
    config_name = 'nltk_tokenizer'
    tagger_id = 'nltk_tokenizer'

    def __init__(self, config):
        super(nltk_tokenizer, self).__init__(config)
        self.sentence_tokenizer = PunktSentenceTokenizer()
        self.word_tokenizer = WhitespaceTokenizer()  # PunktWordTokenizer()
        self.annotator_id = self.config.get('annotator_id', None)

    def _sentences(self, clean_visible):
        'generate strings identified as sentences'
        previous_end = 0
        clean_visible = clean_visible.decode('utf8')
        for start, end in self.sentence_tokenizer.span_tokenize(clean_visible):
            # no need to check start, because the first byte of text
            # is always first byte of first sentence, and we will
            # have already made the previous sentence longer on the
            # end if there was an overlap.
            if start < previous_end:
                start = previous_end
                if start > end:
                    # skip this sentence... because it was eaten by
                    # an earlier sentence with a label
                    continue
            try:
                label = self.label_index.find_le(end)
            except ValueError:
                label = None
            if label:
                ## avoid splitting a label
                off = label.offsets[OffsetType.CHARS]
                end = max(off.first + off.length, end)
            previous_end = end
            sent_str = clean_visible[start:end]
            yield start, end, sent_str

    def make_label_index(self, stream_item):
        'make a sortedcollection on body.labels'
        labels = stream_item.body.labels.get(self.annotator_id)
        if not labels:
            labels = []

        self.label_index = SortedCollection(
            [l for l in labels if OffsetType.CHARS in l.offsets],
            key=lambda label: label.offsets[OffsetType.CHARS].first)

    def make_sentences(self, stream_item):
        'assemble Sentence and Token objects'
        self.make_label_index(stream_item)
        sentences = []
        token_num = 0
        new_mention_id = 0
        for sent_start, sent_end, sent_str in self._sentences(
                stream_item.body.clean_visible):
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
                tok.offsets[OffsetType.CHARS] = Offset(
                    type=OffsetType.CHARS,
                    first=sent_start + start,
                    length=end - start,
                )
                # whitespace tokenizer will never get a token
                # boundary in the middle of an 'author' label
                try:
                    label = self.label_index.find_le(sent_start + start)
                except ValueError:
                    label = None
                if label:
                    off = label.offsets[OffsetType.CHARS]
                    if off.first + off.length > sent_start + start:
                        streamcorpus.add_annotation(tok, label)
                        logger.debug('adding label to tok: %r has %r',
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
        if not stream_item.body.clean_visible:
            return stream_item

        self.label_index = None
        self.label_to_mention_id = dict()
        sentences = self.make_sentences(stream_item)
        stream_item.body.sentences[self.tagger_id] = sentences

        return stream_item
