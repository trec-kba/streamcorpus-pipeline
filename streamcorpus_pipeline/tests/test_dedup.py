from __future__ import absolute_import
from cStringIO import StringIO
import logging
import os
import sys

import yaml
import pytest

from streamcorpus import Chunk
import streamcorpus_pipeline
from streamcorpus_pipeline import Pipeline
from streamcorpus_pipeline._dedup import dedup
from streamcorpus_pipeline.tests._test_data import get_test_chunk_path, \
    get_test_chunk
import yakonfig

logger = logging.getLogger(__name__)

@pytest.mark.xfail
def test_dedup_debugging_config(tmpdir):
    
    ## first test the debugging config
    config_yaml = """
streamcorpus_pipeline:
    dedup:
        ## operate on which part of si.body
        content_form: clean_visible

        ## set this to false for N^2 comparison within each chunk
        require_same_doc_id: False

        use_nilsimsa: True

        ## must be greater than or equal to this
        exactness_nilsimsa_threshold: 128

        ## docs shorter than this are not rejected even if they have
        ## higher than exactness_nilsimsa_threshold with another doc
        min_clean_length: 500

        ## docs with same doc_id that pass exactness_nilsimsa_threshold
        ## are KEPT if the two docs have lengths that differ by more than
        ## this fraction (relative to the longer of the two, measured in
        ## thousandths):
        min_len_sim_thousandths_clean: 850
        min_len_sim_thousandths_raw: 850

        log_dir_path: {}
        log_nilsimsa_threshold: 100,
""".format(tmpdir.dirname)
    with yakonfig.defaulted_config([streamcorpus_pipeline], yaml=config_yaml,
                                   validate=False):
        context = {}
        d1 = dedup()

        num_dups = 0

        for num, si in enumerate(get_test_chunk()):

            if not d1( si, context ):
                num_dups += 1

            if num > 10:
                break

        logger.debug('removed %d near-exact duplicates' % num_dups)
        assert num_dups == 7

def test_dedup_production_config():
    ## now test the production config
    config_yaml = """
streamcorpus_pipeline:
    dedup:
        ## operate on which part of si.body
        content_form: 'clean_visible'

        ## set this to false for N^2 comparison within each chunk
        require_same_doc_id: True

        use_nilsimsa: False

        ## must be greater than or equal to this
        exactness_nilsimsa_threshold: 128

        ## docs shorter than this are not rejected even if they have
        ## higher than exactness_nilsimsa_threshold with another doc
        min_clean_length: 500

        ## docs with same doc_id that pass exactness_nilsimsa_threshold
        ## are KEPT if the two docs have lengths that differ by more than
        ## this fraction (relative to the longer of the two, measured in
        ## thousandths):
        min_len_sim_thousandths_clean: 850
        min_len_sim_thousandths_raw: 850
    """
    with yakonfig.defaulted_config([streamcorpus_pipeline], yaml=config_yaml,
                                   validate=False):
        context = {}
        d1 = dedup()
        num_dups = 0
        for num, si in enumerate(get_test_chunk()):
            if not d1( si, context ):
                num_dups += 1
            if num > 20:
                break
        logger.debug('removed %d near-exact duplicates' % num_dups)
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

