'''rudimentary keyword search indexing build on top of `kvlayer`

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2015 Diffeo, Inc.
'''
from __future__ import absolute_import
from collections import Counter, defaultdict
import logging

import mmh3

from many_stop_words import get_stop_words
from streamcorpus_pipeline._clean_visible import cleanse
from streamcorpus_pipeline._kvlayer_table_names import \
    HASH_TF_INDEX_TABLE, HASH_FREQUENCY_TABLE, HASH_KEYWORD_INDEX_TABLE, \
    kvlayer_key_to_stream_id, key_for_stream_item

logger = logging.getLogger(__name__)

DOCUMENT_HASH_KEY = 0
DOCUMENT_HASH_KEY_REPLACEMENT = 1


class keyword_indexer(object):
    '''Do simple token-based search on documents.

    This is a utility class used by the
    :class:`~streamcorpus_pipeline._kvlayer.to_kvlayer` writer to
    implement simple token-based search.

    .. automethod:: __init__
    .. automethod:: index

    '''

    def __init__(self, kvl, hash_docs=True, hash_frequencies=True,
                 hash_keywords=True, keyword_tagger_ids=None,
                 keyword_size_limit=128):
        '''Create a new keyword indexer.

        `kvl` should be preconfigured to handle the
        :mod:`streamcorpus_pipeline` tables; this constructor will not
        call
        :meth:`~kvlayer._abstract_storage.AbstractStorage.setup_namespace`.
        The various `hash` parameters control what :meth:`index`
        writes out, and if only reading the default values will work.

        :param kvl: :mod:`kvlayer` client object
        :param hash_docs: if true, record hash to document ID index
        :param hash_frequencies: if true, record hash document frequencies
        :param hash_keywords: if true, record hash to keyword reverse index
        :param keyword_tagger_ids: if not :const:`None`, only consider
          these tagger IDs
        :param int keyword_size_limit: largest token to consider

        '''
        self.client = kvl
        self.hash_docs = hash_docs
        self.hash_frequencies = hash_frequencies
        self.hash_keywords = hash_keywords
        self.keyword_tagger_ids = keyword_tagger_ids
        self.keyword_size_limit = keyword_size_limit

    # Class static, initialized on first use
    _stop_words = None

    @property
    def stop_words(self):
        if self._stop_words is None:
            self.__class__._stop_words = get_stop_words()
        return self._stop_words

    def make_hash(self, tok):
        '''Get a Murmur hash for a token.

        `tok` may be a :class:`unicode` string or a UTF-8-encoded
        byte string.  :data:`DOCUMENT_HASH_KEY`, hash value 0, is
        reserved for the document count, and this function remaps
        that value.

        '''
        (_, h) = self.make_hash_kw(tok)
        return h

    def make_hash_kw(self, tok):
        '''Get a Murmur hash and a normalized token.

        `tok` may be a :class:`unicode` string or a UTF-8-encoded
        byte string.  :data:`DOCUMENT_HASH_KEY`, hash value 0, is
        reserved for the document count, and this function remaps
        that value.

        :param tok: token to hash
        :return: pair of normalized `tok` and its hash

        '''
        if isinstance(tok, unicode):
            tok = tok.encode('utf-8')
        h = mmh3.hash(tok)
        if h == DOCUMENT_HASH_KEY:
            h = DOCUMENT_HASH_KEY_REPLACEMENT
        return (tok, h)

    def collect_words(self, si):
        '''Collect all of the words to be indexed from a stream item.

        This scans `si` for all of the configured tagger IDs.  It
        collects all of the token values (the
        :attr:`streamcorpus.Token.token`) and returns a
        :class:`collections.Counter` of them.

        :param si: stream item to scan
        :type si: :class:`streamcorpus.StreamItem`
        :return: counter of :class:`unicode` words to index
        :returntype: :class:`collections.Counter`

        '''
        counter = Counter()
        for tagger_id, sentences in si.body.sentences.iteritems():
            if ((self.keyword_tagger_ids is not None
                 and tagger_id not in self.keyword_tagger_ids)):
                continue
            for sentence in sentences:
                for token in sentence.tokens:
                    term = token.token  # always a UTF-8 byte string
                    term = term.decode('utf-8')
                    term = cleanse(term)
                    if ((self.keyword_size_limit is not None and
                         len(term) > self.keyword_size_limit)):
                        continue
                    if term not in self.stop_words:
                        counter[term] += 1
        return counter

    def index(self, si):
        '''Record index records for a single document.

        Which indexes this creates depends on the parameters to the
        constructor.  This records all of the requested indexes for
        a single document.

        '''
        if not si.body.clean_visible:
            logger.warn('stream item %s has no clean_visible part, '
                        'skipping keyword indexing', si.stream_id)
            return

        # Count tokens in si.clean_visible
        # We will recycle hash==0 for "# of documents"
        hash_counts = defaultdict(int)
        hash_counts[DOCUMENT_HASH_KEY] = 1
        hash_kw = defaultdict(int)
        words = self.collect_words(si)
        for tok, count in words.iteritems():
            (tok, tok_hash) = self.make_hash_kw(tok)
            hash_counts[tok_hash] += count
            hash_kw[tok] = tok_hash

        # Convert this and write it out
        if self.hash_docs:
            (k1, k2) = key_for_stream_item(si)
            kvps = [((h, k1, k2), n) for (h, n) in hash_counts.iteritems()
                    if h != DOCUMENT_HASH_KEY]
            self.client.put(HASH_TF_INDEX_TABLE, *kvps)

        if self.hash_frequencies:
            kvps = [((h,), 1) for h in hash_counts.iterkeys()]
            self.client.increment(HASH_FREQUENCY_TABLE, *kvps)

        if self.hash_keywords:
            kvps = [((h, t), 1) for (t, h) in hash_kw.iteritems()]
            self.client.increment(HASH_KEYWORD_INDEX_TABLE, *kvps)

    def invert_hash(self, tok_hash):
        '''Get strings that correspond to some hash.

        No string will correspond to :data:`DOCUMENT_HASH_KEY`; use
        :data:`DOCUMENT_HASH_KEY_REPLACEMENT` instead.

        :param int tok_hash: Murmur hash to query
        :return: list of :class:`unicode` strings

        '''
        return [tok_encoded.decode('utf8')
                for (_, tok_encoded) in
                self.client.scan_keys(HASH_KEYWORD_INDEX_TABLE,
                                      ((tok_hash,), (tok_hash,)))]

    def document_frequencies(self, hashes):
        '''Get document frequencies for a list of hashes.

        This will return all zeros unless the index was written with
        `hash_frequencies` set.  If :data:`DOCUMENT_HASH_KEY` is
        included in `hashes`, that value will be returned with the
        total number of documents indexed.  If you are looking for
        documents with that hash, pass
        :data:`DOCUMENT_HASH_KEY_REPLACEMENT` instead.

        :param hashes: hashes to query
        :paramtype hashes: list of :class:`int`
        :return: map from hash to document frequency

        '''
        result = {}
        for (k, v) in self.client.get(HASH_FREQUENCY_TABLE,
                                      *[(h,) for h in hashes]):
            if v is None:
                v = 0
            result[k[0]] = v
        return result

    def lookup(self, h):
        '''Get stream IDs for a single hash.

        This yields strings that can be retrieved using
        :func:`streamcorpus_pipeline._kvlayer.get_kvlayer_stream_item`,
        or fed back into :mod:`coordinate` or other job queue systems.

        Note that for common terms this can return a large number of
        stream IDs!  This is a scan over a dense region of a
        :mod:`kvlayer` table so it should be reasonably efficient,
        but be prepared for it to return many documents in a large
        corpus.  Blindly storing the results in a :class:`list`
        may be inadvisable.

        This will return nothing unless the index was written with
        :attr:`hash_docs` set.  No document will correspond to
        :data:`DOCUMENT_HASH_KEY`; use
        :data:`DOCUMENT_HASH_KEY_REPLACEMENT` instead.

        :param int h: Murmur hash to look up

        '''
        for (_, k1, k2) in self.client.scan_keys(HASH_TF_INDEX_TABLE,
                                                 ((h,), (h,))):
            yield kvlayer_key_to_stream_id((k1, k2))

    def lookup_tf(self, h):
        '''Get stream IDs and term frequencies for a single hash.

        This yields pairs of strings that can be retrieved using
        :func:`streamcorpus_pipeline._kvlayer.get_kvlayer_stream_item`
        and the corresponding term frequency.

        ..see:: :meth:`lookup`


        '''
        for ((_, k1, k2), v) in self.client.scan(HASH_TF_INDEX_TABLE,
                                                 ((h,), (h,))):
            yield (kvlayer_key_to_stream_id((k1, k2)), v)
