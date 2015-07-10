'''tests for keyword indexing

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2015 Diffeo, Inc.

'''
from __future__ import division, absolute_import

import pytest

import kvlayer
from kvlayer._local_memory import LocalStorage
from streamcorpus import Chunk, make_stream_item, Sentence, Token
import streamcorpus_pipeline
from streamcorpus_pipeline._kvlayer import to_kvlayer
from streamcorpus_pipeline._kvlayer_table_names import STREAM_ITEM_TABLE_DEFS, \
    STREAM_ITEM_VALUE_DEFS, HASH_TF_INDEX_TABLE, HASH_FREQUENCY_TABLE, \
    HASH_KEYWORD_INDEX_TABLE, stream_id_to_kvlayer_key
import yakonfig

mmh3 = pytest.importorskip('mmh3')
_kvlayer_keyword_search = pytest.importorskip(
    'streamcorpus_pipeline._kvlayer_keyword_search')
keyword_indexer = _kvlayer_keyword_search.keyword_indexer
DOCUMENT_HASH_KEY = _kvlayer_keyword_search.DOCUMENT_HASH_KEY


@pytest.fixture
def kvlclient():
    client = LocalStorage(app_name='a', namespace='n')
    client._data = {}  # instance-specific
    client.setup_namespace(STREAM_ITEM_TABLE_DEFS, STREAM_ITEM_VALUE_DEFS)
    return client


@pytest.fixture
def indexer(kvlclient):
    return keyword_indexer(kvlclient)


@pytest.fixture(scope='session')
def phrases():
    '''list of "documents" to test on'''
    return [
        'quick brown fox',
        'lazy dog',
        'lazy programmer',
        'dog eat dog',
    ]


@pytest.fixture(scope='session')
def corpus(phrases):
    corpus = {}
    for phrase in phrases:
        initials = [word[0] for word in phrase.split()]
        abs_url = 'test:///' + ''.join(initials)
        # Fri Feb 13 23:31:30 UTC 2009
        si = make_stream_item(1234567890, abs_url)
        si.body.raw = phrase
        si.body.clean_visible = phrase
        si.body.sentences['test'] = [Sentence(tokens=[
            Token(token_num=n, token=word, sentence_pos=n)
            for (n, word) in enumerate(phrase.split())
        ])]
        corpus[phrase] = si
    return corpus


def test_index_one(corpus, indexer, kvlclient):
    si = corpus['quick brown fox']
    indexer.index(si)
    (k1, k2) = stream_id_to_kvlayer_key(si.stream_id)

    # This should create keywords "quick", "brown", "fox" with frequency 1 each
    # By inspection this is the correct order and none are 0
    assert list(kvlclient.scan(HASH_TF_INDEX_TABLE)) == [
        ((mmh3.hash('fox'), k1, k2), 1),
        ((mmh3.hash('brown'), k1, k2), 1),
        ((mmh3.hash('quick'), k1, k2), 1)]
    assert list(kvlclient.scan(HASH_FREQUENCY_TABLE)) == [
        ((mmh3.hash('fox'),), 1),
        ((DOCUMENT_HASH_KEY,), 1),
        ((mmh3.hash('brown'),), 1),
        ((mmh3.hash('quick'),), 1)]
    assert list(kvlclient.scan(HASH_KEYWORD_INDEX_TABLE)) == [
        ((mmh3.hash('fox'), 'fox'), 1),
        ((mmh3.hash('brown'), 'brown'), 1),
        ((mmh3.hash('quick'), 'quick'), 1)]


def test_index_two(corpus, indexer, kvlclient):
    si1 = corpus['lazy dog']
    si2 = corpus['lazy programmer']
    indexer.index(si1)
    indexer.index(si2)
    k1a, k1b = stream_id_to_kvlayer_key(si1.stream_id)
    k2a, k2b = stream_id_to_kvlayer_key(si2.stream_id)

    assert list(kvlclient.scan(HASH_TF_INDEX_TABLE)) == [
        ((mmh3.hash('dog'), k1a, k1b), 1),
        ((mmh3.hash('lazy'), k1a, k1b), 1),
        ((mmh3.hash('lazy'), k2a, k2b), 1),
        ((mmh3.hash('programmer'), k2a, k2b), 1)]
    assert list(kvlclient.scan(HASH_FREQUENCY_TABLE)) == [
        ((mmh3.hash('dog'),), 1),
        ((mmh3.hash('lazy'),), 2),
        ((mmh3.hash('programmer'),), 1),
        ((DOCUMENT_HASH_KEY,), 2)]
    assert list(kvlclient.scan(HASH_KEYWORD_INDEX_TABLE)) == [
        ((mmh3.hash('dog'), 'dog'), 1),
        ((mmh3.hash('lazy'), 'lazy'), 2),
        ((mmh3.hash('programmer'), 'programmer'), 1)]


def test_index_dog(corpus, indexer, kvlclient):
    si1 = corpus['lazy dog']
    si2 = corpus['dog eat dog']
    indexer.index(si1)
    indexer.index(si2)
    k1a, k1b = stream_id_to_kvlayer_key(si1.stream_id)
    k2a, k2b = stream_id_to_kvlayer_key(si2.stream_id)

    assert list(kvlclient.scan(HASH_TF_INDEX_TABLE)) == [
        ((mmh3.hash('dog'), k1a, k1b), 1),
        ((mmh3.hash('dog'), k2a, k2b), 2),
        ((mmh3.hash('lazy'), k1a, k1b), 1),
        ((mmh3.hash('eat'), k2a, k2b), 1)]
    assert list(kvlclient.scan(HASH_FREQUENCY_TABLE)) == [
        ((mmh3.hash('dog'),), 2),
        ((mmh3.hash('lazy'),), 1),
        ((DOCUMENT_HASH_KEY,), 2),
        ((mmh3.hash('eat'),), 1)]
    assert list(kvlclient.scan(HASH_KEYWORD_INDEX_TABLE)) == [
        ((mmh3.hash('dog'), 'dog'), 2),
        ((mmh3.hash('lazy'), 'lazy'), 1),
        ((mmh3.hash('eat'), 'eat'), 1)]


def test_index_limit(corpus, indexer, kvlclient):
    si = corpus['lazy programmer']
    indexer.keyword_size_limit = 8
    indexer.index(si)
    (k1, k2) = stream_id_to_kvlayer_key(si.stream_id)

    # This should only capture "lazy", "programmer" is too long
    assert list(kvlclient.scan(HASH_TF_INDEX_TABLE)) == [
        ((mmh3.hash('lazy'), k1, k2), 1),
    ]
    assert list(kvlclient.scan(HASH_FREQUENCY_TABLE)) == [
        ((mmh3.hash('lazy'),), 1),
        ((DOCUMENT_HASH_KEY,), 1),
    ]
    assert list(kvlclient.scan(HASH_KEYWORD_INDEX_TABLE)) == [
        ((mmh3.hash('lazy'), 'lazy'), 1),
    ]


def test_invert(corpus, indexer):
    si = corpus['lazy dog']
    indexer.index(si)

    assert indexer.invert_hash(mmh3.hash('lazy')) == ['lazy']
    assert indexer.invert_hash(mmh3.hash('dog')) == ['dog']
    assert indexer.invert_hash(DOCUMENT_HASH_KEY) == []
    assert indexer.invert_hash(mmh3.hash('programmer')) == []


def test_document_frequencies(corpus, indexer):
    for si in corpus.itervalues():
        indexer.index(si)

    assert indexer.document_frequencies([DOCUMENT_HASH_KEY]) == {
        DOCUMENT_HASH_KEY: len(corpus),
    }
    assert indexer.document_frequencies([indexer.make_hash('fox')]) == {
        mmh3.hash('fox'): 1,
    }
    all_words = ['quick', 'brown', 'fox', 'lazy', 'dog', 'programmer', 'eat']
    assert (indexer.document_frequencies(
        [DOCUMENT_HASH_KEY] + [indexer.make_hash(k) for k in all_words]
    ) == {
        DOCUMENT_HASH_KEY: 4,
        mmh3.hash('quick'): 1,
        mmh3.hash('brown'): 1,
        mmh3.hash('fox'): 1,
        mmh3.hash('lazy'): 2,
        mmh3.hash('dog'): 2,
        mmh3.hash('programmer'): 1,
        mmh3.hash('eat'): 1,
    })


def test_lookup(corpus, indexer):
    for si in corpus.itervalues():
        indexer.index(si)

    assert list(indexer.lookup(DOCUMENT_HASH_KEY)) == []
    assert list(indexer.lookup(17)) == []
    assert list(indexer.lookup(indexer.make_hash('quick'))) == [
        corpus['quick brown fox'].stream_id,
    ]
    assert list(indexer.lookup(indexer.make_hash('lazy'))) == [
        corpus['lazy dog'].stream_id,
        corpus['lazy programmer'].stream_id,
    ]
    assert list(indexer.lookup(indexer.make_hash('dog'))) == [
        corpus['lazy dog'].stream_id,
        corpus['dog eat dog'].stream_id,
    ]


def test_lookup_tf(corpus, indexer):
    for si in corpus.itervalues():
        indexer.index(si)

    assert list(indexer.lookup_tf(DOCUMENT_HASH_KEY)) == []
    assert list(indexer.lookup_tf(17)) == []
    assert list(indexer.lookup_tf(indexer.make_hash('quick'))) == [
        (corpus['quick brown fox'].stream_id, 1),
    ]
    assert list(indexer.lookup_tf(indexer.make_hash('lazy'))) == [
        (corpus['lazy dog'].stream_id, 1),
        (corpus['lazy programmer'].stream_id, 1),
    ]
    assert list(indexer.lookup_tf(indexer.make_hash('dog'))) == [
        (corpus['lazy dog'].stream_id, 1),
        (corpus['dog eat dog'].stream_id, 2),
    ]


def test_e2e_writer(namespace_string, corpus, tmpdir):
    t_path = str(tmpdir.join('chunk.sc'))
    with Chunk(path=t_path, mode='wb') as chunk:
        for si in corpus.itervalues():
            chunk.add(si)

    with yakonfig.defaulted_config([kvlayer, streamcorpus_pipeline], config={
            'kvlayer': {
                'storage_type': 'local',
                'app_name': 'a',
                'namespace': namespace_string,
            },
            'streamcorpus_pipeline': {
                'writers': ['to_kvlayer'],
                'to_kvlayer': {
                    'indexes': ['keywords'],
                },
            },
    }) as config:
        writer = None
        try:
            writer = to_kvlayer(config['streamcorpus_pipeline']['to_kvlayer'])
            assert writer.keyword_indexer
            writer(t_path, {}, '')
            assert list(writer.keyword_indexer.lookup(
                writer.keyword_indexer.make_hash('quick')
            )) == [
                corpus['quick brown fox'].stream_id,
            ]
            assert list(writer.keyword_indexer.lookup(
                writer.keyword_indexer.make_hash('dog')
            )) == [
                corpus['lazy dog'].stream_id,
                corpus['dog eat dog'].stream_id,
            ]
        finally:
            if writer and writer.client:
                writer.client.delete_namespace()
