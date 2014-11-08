'''tests for keyword indexing speed 

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2014 Diffeo, Inc.
'''
from __future__ import division, absolute_import

import logging
import time

import kvlayer
import pytest
import streamcorpus
import yakonfig

import streamcorpus_pipeline
from streamcorpus_pipeline._kvlayer import from_kvlayer, to_kvlayer
from streamcorpus_pipeline._kvlayer_keyword_search import keyword_indexer, keywords
from streamcorpus_pipeline._kvlayer_table_names import table_name, all_tables, \
    WITH_SOURCE, DOC_ID_EPOCH_TICKS, HASH_KEYWORD, HASH_TF_SID
from streamcorpus_pipeline.tests._test_data import get_test_v0_3_0_chunk_path
from streamcorpus_pipeline._kvlayer_table_names import all_tables
from streamcorpus_pipeline.tests.test_kvlayer import configurator


logger = logging.getLogger(__name__)


@pytest.mark.slow
def test_keywords_throughput(configurator, test_data_dir):
    overlay = {
        'streamcorpus_pipeline': {
            'to_kvlayer': {
                'indexes': [ HASH_KEYWORD, HASH_TF_SID ],
            },
        },
    }

    with configurator(overlay):
        path = get_test_v0_3_0_chunk_path(test_data_dir)
        config = yakonfig.get_global_config('streamcorpus_pipeline',
                                            'to_kvlayer')
        profile_chunk(path)

def profile_chunk(path):
    client = kvlayer.client()
    client.setup_namespace(all_tables())

    indexer = keyword_indexer(client)

    input_SIs = list(streamcorpus.Chunk(path))
    hash_to_word = dict()

    start = time.time()
    num_tokens = 0
    num_bytes = 0
    num_tok_bytes = 0
    for si in input_SIs:
        num_bytes += si.body.clean_visible and len(si.body.clean_visible) or 0
        for tok in keywords(si, indexer.analyzer, hash_to_word):
            num_tokens += 1
            num_tok_bytes += len(tok)
    elapsed = time.time() - start
    logger.info('%d SIs, %.1f clean_visible MB, %d tokens, %.1f index MB in %d seconds'
                ' --> %.1f SI/sec, %.1f MB/sec, %.1f tokens/sec, %.1f index MB/sec',
                len(input_SIs), num_bytes / 2**20, num_tokens,
                num_tok_bytes / 2**20,
                elapsed,
                len(input_SIs) / elapsed, 
                num_bytes / 2**20 / elapsed,
                num_tokens / elapsed,
                num_tok_bytes / 2**20 / elapsed,
    )


if __name__ == '__main__':
    import argparse

    import dblogger
    import yakonfig

    parser = argparse.ArgumentParser(description='profile of keyword indexing components on an input chunk file')
    parser.add_argument('path', help='path to chunk file, which will be loaded entirely into memory to check in-memory performance of processing steps for keyword indexing')
    
    modules = [yakonfig, kvlayer, dblogger, streamcorpus_pipeline]
    args = yakonfig.parse_args(parser, modules)

    logger.info('running with %s', args.path)

    profile_chunk(args.path)
