from __future__ import absolute_import
import pytest
from streamcorpus import Chunk, make_stream_item, add_annotation, \
    Sentence, Token, Annotator, Target, Rating

import streamcorpus_pipeline.stages
from streamcorpus_pipeline.tests._test_data import \
    get_john_smith_tagged_by_lingpipe_without_labels_data

from streamcorpus_pipeline._taggers import multi_token_match, \
    look_ahead_match

@pytest.fixture(scope='module')
def stages():
    return streamcorpus_pipeline.stages.PipelineStages()

@pytest.mark.parametrize("tagger,chain_selector", [
    ('name_align_labels', 'ALL'),
    pytest.mark.skipif('True', ('line_offset_align_labels', 'ALL')),
    ('byte_offset_align_labels', 'ALL'),
    ('name_align_labels', 'ANY_MULTI_TOKEN'),
])
def test_tagger_transform(tagger, chain_selector, stages, tmpdir, test_data_dir):
    transform = stages.init_stage(tagger, { tagger: {
        'tagger_id': 'lingpipe',
        'annotator_id': 'bagga-and-baldwin',
        'chain_selector': chain_selector
    }})
    data = get_john_smith_tagged_by_lingpipe_without_labels_data(test_data_dir)
    with tmpdir.join('{0}.{1}.sc'.format(tagger, chain_selector)).open('wb') as tf:
        tf.write(data)
        tf.flush()
        transform.process_path(tf.name)
        found_one = False
        for si in Chunk(tf.name):
            for sentence in si.body.sentences['lingpipe']:
                for token in sentence.tokens:
                    if token.labels:
                        found_one = True
        assert found_one


def test_multi_token_match():
    si = make_stream_item(0, '')
    tagger_id = 'test_tagger'
    annotator_id = 'test_anno'
    target_id = 'test_target'
    si.body.sentences[tagger_id] = [
        Sentence(tokens=[
                Token(token='This'),
                Token(token='-LRB-big-RRB- dog'),
                Token(token='Jake'),
                Token(token='has'),
                Token(token='no'),
                Token(token=u'\u1F601'.encode('utf8')),
                Token(token='...'),
                Token(token='Teeth'),
                ])]
    rating = Rating(annotator=Annotator(annotator_id=annotator_id),
           target=Target(target_id=target_id),
           mentions=['Big dog! Jake... ', u'\u1F601 Teeth'.encode('utf8')],
           )
    add_annotation(si, rating)
    aligner_data = dict(
        tagger_id = tagger_id,
        annotator_id = annotator_id,
        )
                               
    multi_token_match(si, aligner_data)

    assert si.body.sentences[tagger_id][0].tokens[1].labels
    assert si.body.sentences[tagger_id][0].tokens[2].labels
    assert si.body.sentences[tagger_id][0].tokens[-3].labels
    assert si.body.sentences[tagger_id][0].tokens[-2].labels
    assert si.body.sentences[tagger_id][0].tokens[-1].labels


def test_look_ahead_match():
    '''Verify that look_ahead_match detects tokens based on both regexes
    and string match.

    '''

    rating = Rating(mentions=['''John ur"^Smith[a-z]*$"''', '''bob'''])
    tokens = [(['The'], False),
              (['cat'], False),
              (['and'], False),
              (['the'], False),
              (['John'], 1),
              (['Smithee'], 2),
              (['John'], 3),
              (['Smith'], 4),
              (['said'], False),
              (['hi'], False),
              (['to'], False),
              (['Bob'], 5),
              (['.'], False),
              ]

    for boolean in look_ahead_match(rating, tokens):
        assert boolean

    assert set(look_ahead_match(rating, tokens)) == set([1, 2, 3, 4, 5])
