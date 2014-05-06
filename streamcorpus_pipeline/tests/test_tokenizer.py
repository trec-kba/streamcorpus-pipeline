from __future__ import absolute_import
import os
from streamcorpus import Chunk, serialize
from streamcorpus_pipeline._tokenizer import nltk_tokenizer

def test_tokenizer(test_data_dir):
    path = os.path.join(test_data_dir, 'test', 'wlc-chunk-with-labels.sc' )
    num = 0
    for si in Chunk(path):
        num += 1
        ## there is only one StreamItem in this chunk
        sentences = si.body.sentences.pop('nltk_tokenizer')
        t = nltk_tokenizer(config={'annotator_id': 'author'})
        t.process_item(si)

        assert num > 0

        ## if something changes, then need to save new test data
        #open(path, 'wb').write(serialize(si))
        #return
        if 1:

            #assert si.body.sentences['nltk_tokenizer'] == sentences
            num = 0
            for i in range(len(si.body.sentences['nltk_tokenizer'])):
                for j in range(len(si.body.sentences['nltk_tokenizer'][i].tokens)):
                    tok_t = si.body.sentences['nltk_tokenizer'][i].tokens[j]
                    for attr in dir(tok_t):
                        if attr.startswith('__'): continue #type(attr) == type(test_tokenizer): continue
                        ## printing for diagnostics when things change
                        #print 'checking ', attr
                    assert getattr(tok_t, attr)  == getattr(sentences[i].tokens[j], attr)
                    num += 1

            assert num > 0

        ## check something about mention_id?
