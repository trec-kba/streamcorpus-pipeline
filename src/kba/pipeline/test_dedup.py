
import os
import sys
import yaml
import pytest
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

from _test_data import get_test_chunk_path, get_test_chunk

def test_dedup_debugging_config(tmpdir):
    
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

        log_dir_path = tmpdir.dirname,
        log_nilsimsa_threshold = 100,
        )

    context = {}
    d1 = dedup( config )

    num_dups = 0

    for num, si in enumerate(get_test_chunk()):

        if not d1( si, context ):
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

    context = {}

    d1 = dedup( config )

    num_dups = 0

    for num, si in enumerate(get_test_chunk()):

        if not d1( si, context ):
            num_dups += 1


        if num > 20:
            break
    print 'removed %d near-exact duplicates' % num_dups
    assert num_dups == 3

## until we get v0_3_0 data into _test_data.py
@pytest.mark.xfail  # pylint: disable=E1101  
def test_dedup_chunk_counts():
    path = os.path.dirname(__file__)
    config = yaml.load(open(os.path.join(path, 'test_dedup_chunk_counts.yaml')))

    ## config says read from stdin, so make that have what we want
    stdin = sys.stdin
    sys.stdin = StringIO(get_test_chunk_path())

    ## run the pipeline
    p = Pipeline( config )
    p.run()

