'''rudimentary command-line interface for keyword search functionality

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2014 Diffeo, Inc.
'''
from __future__ import absolute_import
from streamcorpus_pipeline._kvlayer_table_names import STREAM_ITEM_TABLE_DEFS
from streamcorpus_pipeline._kvlayer_keyword_search import keyword_indexer
import kvlayer
import yakonfig

def main():
    import streamcorpus_pipeline
    import argparse
    parser = argparse.ArgumentParser(description='basic command-line tool for checking indexes built by streamcorpus_pipeline._to_kvlayer.  Accepts a query string as input and print stream_ids of documents that match *any* of the terms')
    parser.add_argument('query_string', nargs='+', help='enter one or more query words; will be combined with OR')

    modules = [yakonfig, kvlayer]
    args = yakonfig.parse_args(parser, modules)
    config = yakonfig.get_global_config()

    kvl = kvlayer.client()
    kvl.setup_namespace(STREAM_ITEM_TABLE_DEFS)

    indexer = keyword_indexer(kvl)

    for si in indexer.search(' '.join(args.query_string)):
        print si.stream_id


