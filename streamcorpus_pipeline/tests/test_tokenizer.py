
from __future__ import absolute_import
from streamcorpus import Annotator, Label, make_stream_item, Offset, \
    OffsetType, Target
from streamcorpus_pipeline._tokenizer import nltk_tokenizer

def test_tokenizer():
    si = make_stream_item('2014-01-21T13:00:00.000Z',
                          'file:///test_tokenizer.py')
    si.body.clean_visible = ('The quick brown fox jumped over the lazy dog.  '
                             'This is only a test.')
    t = nltk_tokenizer(config={})
    si = t.process_item(si, {})
    assert si is not None
    assert 'nltk_tokenizer' in si.body.sentences
    sentences = si.body.sentences['nltk_tokenizer']
    assert len(sentences) == 2

    assert [tok.token_num for tok in sentences[0].tokens] == range(9)
    assert [tok.sentence_pos for tok in sentences[0].tokens] == range(9)
    assert ([tok.token for tok in sentences[0].tokens] ==
            ['The', 'quick', 'brown', 'fox', 'jumped', 'over', 'the',
             'lazy', 'dog.'])
    assert all(OffsetType.CHARS in tok.offsets for tok in sentences[0].tokens)
    assert ([tok.offsets[OffsetType.CHARS].first
             for tok in sentences[0].tokens] ==
            [0, 4, 10, 16, 20, 27, 32, 36, 41])
    assert ([tok.offsets[OffsetType.CHARS].length
             for tok in sentences[0].tokens] ==
            [3, 5, 5, 3, 6, 4, 3, 4, 4])
    assert all(tok.mention_id == -1 for tok in sentences[0].tokens)

    assert [tok.token_num for tok in sentences[1].tokens] == range(9, 14)
    assert [tok.sentence_pos for tok in sentences[1].tokens] == range(5)
    assert ([tok.token for tok in sentences[1].tokens] ==
            ['This', 'is', 'only', 'a', 'test.'])
    assert all(OffsetType.CHARS in tok.offsets for tok in sentences[1].tokens)
    assert ([tok.offsets[OffsetType.CHARS].first
             for tok in sentences[1].tokens] ==
            [47, 52, 55, 60, 62])
    assert ([tok.offsets[OffsetType.CHARS].length
             for tok in sentences[1].tokens] ==
            [4, 2, 4, 1, 5])
    assert all(tok.mention_id == -1 for tok in sentences[1].tokens)


def test_tokenizer_align():
    si = make_stream_item('2014-01-21T13:00:00.000Z',
                          'file:///test_tokenizer.py')
    si.body.clean_visible = ('The president is John.')
    label = Label(annotator=Annotator(annotator_id='author'),
                  target=Target(target_id='john'),
                  offsets={OffsetType.CHARS:
                           Offset(type=OffsetType.CHARS,
                                  first=17,
                                  length=4)})
    si.body.labels = {'author': [label]}
    t = nltk_tokenizer(config={'annotator_id': 'author'})
    si = t.process_item(si, {})
    assert si is not None
    assert 'nltk_tokenizer' in si.body.sentences
    sentences = si.body.sentences['nltk_tokenizer']
    assert len(sentences) == 1
    assert [tok.offsets[OffsetType.CHARS].first
            for tok in sentences[0].tokens] == [0, 4, 14, 17]
    assert sentences[0].tokens[3].token == 'John.'
    assert sentences[0].tokens[1].labels == {}
    assert sentences[0].tokens[1].mention_id == -1
    assert sentences[0].tokens[3].labels == {'author': [label]}
    assert sentences[0].tokens[3].mention_id == 0


def test_tokenizer_dont_align_by_default():
    si = make_stream_item('2014-01-21T13:00:00.000Z',
                          'file:///test_tokenizer.py')
    si.body.clean_visible = ('The president is John.')
    label = Label(annotator=Annotator(annotator_id='author'),
                  target=Target(target_id='john'),
                  offsets={OffsetType.CHARS:
                           Offset(type=OffsetType.CHARS,
                                  first=17,
                                  length=4)})
    si.body.labels = {'author': [label]}
    t = nltk_tokenizer(config={})
    si = t.process_item(si, {})
    assert all(tok.labels == {}
               for tok in si.body.sentences['nltk_tokenizer'][0].tokens)


def test_tokenizer_require_char_offset():
    si = make_stream_item('2014-01-21T13:00:00.000Z',
                          'file:///test_tokenizer.py')
    si.body.clean_visible = ('The president is John.')
    label = Label(annotator=Annotator(annotator_id='author'),
                  target=Target(target_id='john'),
                  offsets={OffsetType.BYTES:
                           Offset(type=OffsetType.BYTES,
                                  first=17,
                                  length=4)})
    si.body.labels = {'author': [label]}
    t = nltk_tokenizer(config={'annottator_id': 'author'})
    si = t.process_item(si, {})
    assert all(tok.labels == {}
               for tok in si.body.sentences['nltk_tokenizer'][0].tokens)
