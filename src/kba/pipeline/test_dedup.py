
import os
import sys
import yaml
from cStringIO import StringIO
from streamcorpus import Chunk
from _pipeline import Pipeline
from _dedup import dedup

import logging
logger = logging.getLogger('kba')
logger.setLevel( logging.DEBUG )

ch = logging.StreamHandler()
ch.setLevel( logging.DEBUG )
#ch.setFormatter(formatter)
logger.addHandler(ch)

def get_test_chunk_path():
    path = os.path.dirname(__file__)
    path = os.path.join( path, '../../../data/test/', 'WEBLOG-100-fd5f05c8a680faa2bf8c55413e949bbf.sc' )
    return path

def get_test_chunk():
    return Chunk(path=get_test_chunk_path())
    return stream_item

def test_dedup_debugging_config():
    
    ## first test the debugging config
    config = dict(
        ## operate on which part of si.body
        content_form = 'clean_visible',

        ## set this to false for N^2 comparison within each chunk
        require_same_doc_id = False,

        use_nilsimsa = True,

        ## must be greater than or equal to this
        exactness_nilsimsa_threshold = 128,

        ## docs shorter than this are not rejected even if they have
        ## higher than exactness_nilsimsa_threshold with another doc
        min_clean_length = 500,

        ## docs with same doc_id that pass exactness_nilsimsa_threshold
        ## are KEPT if the two docs have lengths that differ by more than
        ## this fraction (relative to the longer of the two, measured in
        ## thousandths):
        min_len_sim_thousandths_clean = 850,
        min_len_sim_thousandths_raw = 850,

        log_dir_path = '/tmp/foo',
        log_nilsimsa_threshold = 100,
        )

    d1 = dedup( config )

    num_dups = 0

    for num, si in enumerate(get_test_chunk()):

        if not d1( si ):
            num_dups += 1

        if num > 10:
            break

    print 'removed %d near-exact duplicates' % num_dups
    assert num_dups == 7

def test_dedup_production_config():
    ## now test the production config
    config = dict(
        ## operate on which part of si.body
        content_form = 'clean_visible',

        ## set this to false for N^2 comparison within each chunk
        require_same_doc_id = True,

        use_nilsimsa = False,

        ## must be greater than or equal to this
        exactness_nilsimsa_threshold = 128,

        ## docs shorter than this are not rejected even if they have
        ## higher than exactness_nilsimsa_threshold with another doc
        min_clean_length = 500,

        ## docs with same doc_id that pass exactness_nilsimsa_threshold
        ## are KEPT if the two docs have lengths that differ by more than
        ## this fraction (relative to the longer of the two, measured in
        ## thousandths):
        min_len_sim_thousandths_clean = 850,
        min_len_sim_thousandths_raw = 850,
        )

    d1 = dedup( config )

    num_dups = 0

    for num, si in enumerate(get_test_chunk()):

        if not d1( si ):
            num_dups += 1


        if num > 20:
            break
    print 'removed %d near-exact duplicates' % num_dups
    assert num_dups == 3

def test_dedup_chunk_counts():
    path = os.path.dirname(__file__)
    config = yaml.load(open(os.path.join(path, 'test_dedup_chunk_counts.yaml')))

    ## config says read from stdin, so make that have what we want
    stdin = sys.stdin
    sys.stdin = StringIO(get_test_chunk_path())

    ## run the pipeline
    p = Pipeline( config )
    p.run()

