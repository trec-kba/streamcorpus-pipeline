from __future__ import absolute_import
import logging

import pytest

import streamcorpus_pipeline
from streamcorpus_pipeline._dedup import dedup
from streamcorpus_pipeline._pipeline import PipelineFactory
from streamcorpus_pipeline.stages import PipelineStages
from streamcorpus_pipeline.tests._test_data import get_test_chunk_path, \
    get_test_chunk
import yakonfig

logger = logging.getLogger(__name__)

def test_dedup_debugging_config(test_data_dir, tmpdir):

    ## first test the debugging config
    context = {}
    d1 = dedup({
        ## operate on which part of si.body
        'content_form': 'clean_visible',

        ## set this to false for N^2 comparison within each chunk
        'require_same_doc_id': False,

        'use_nilsimsa': True,

        ## must be greater than or equal to this
        'exactness_nilsimsa_threshold': 128,

        ## docs shorter than this are not rejected even if they have
        ## higher than exactness_nilsimsa_threshold with another doc
        'min_clean_length': 500,

        ## docs with same doc_id that pass exactness_nilsimsa_threshold
        ## are KEPT if the two docs have lengths that differ by more than
        ## this fraction (relative to the longer of the two, measured in
        ## thousandths):
        'min_len_sim_thousandths_clean': 850,
        'min_len_sim_thousandths_raw': 850,

        'log_dir_path': tmpdir.dirname,
        'log_nilsimsa_threshold': 100,
    })

    num_dups = 0

    for num, si in enumerate(get_test_chunk(test_data_dir)):

        if not d1( si, context ):
            num_dups += 1

        if num > 10:
            break

    logger.debug('removed %d near-exact duplicates' % num_dups)
    assert num_dups == 7

def test_dedup_production_config(test_data_dir):
    ## now test the production config
    context = {}
    d1 = dedup({
        ## operate on which part of si.body
        'content_form': 'clean_visible',

        ## set this to false for N^2 comparison within each chunk
        'require_same_doc_id': True,

        'use_nilsimsa': False,

        ## must be greater than or equal to this
        'exactness_nilsimsa_threshold': 128,

        ## docs shorter than this are not rejected even if they have
        ## higher than exactness_nilsimsa_threshold with another doc
        'min_clean_length': 500,

        ## docs with same doc_id that pass exactness_nilsimsa_threshold
        ## are KEPT if the two docs have lengths that differ by more than
        ## this fraction (relative to the longer of the two, measured in
        ## thousandths):
        'min_len_sim_thousandths_clean': 850,
        'min_len_sim_thousandths_raw': 850,
    })
    num_dups = 0
    for num, si in enumerate(get_test_chunk(test_data_dir)):
        if not d1( si, context ):
            num_dups += 1
        if num > 20:
            break
    logger.debug('removed %d near-exact duplicates' % num_dups)
    assert num_dups == 3

@pytest.mark.slow
def test_dedup_chunk_counts(request, test_data_dir, tmpdir):
    filename = str(request.fspath.dirpath('test_dedup_chunk_counts.yaml'))
    with yakonfig.defaulted_config([streamcorpus_pipeline],
                                   filename=filename,
                                   config={'tmp_dir_path': str(tmpdir)}
    ) as config:
        ## run the pipeline
        pf = PipelineFactory(PipelineStages())
        p = pf(config['streamcorpus_pipeline'])
        p.run(get_test_chunk_path(test_data_dir))
