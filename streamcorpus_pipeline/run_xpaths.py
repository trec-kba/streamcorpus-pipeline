from __future__ import absolute_import, division, print_function

import argparse
import copy
import os.path as path

import dblogger
import streamcorpus_pipeline
from streamcorpus_pipeline.stages import PipelineStages
from streamcorpus_pipeline._pipeline import PipelineFactory
import yakonfig


default_config = {
    'logging': {
        'root': {
            'level': 'DEBUG',
        },
    },
    'streamcorpus_pipeline': {
        'incremental_transforms': [
            'guess_media_type',
            'pdf_to_text', 'docx_to_text', 'language', 'clean_html',
            'clean_visible', 'force_clean_html', 'hashrash_nilsimsa',
            'title', 'basis',
        ],
        'batch_transforms': ['multi_token_match_align_labels'],
        'post_batch_incremental_transforms': ['xpath_offsets'],
        'reader': 'from_local_files',
        'writers': ['to_local_chunks'],
        'guess_media_type': {
            'fallback_media_type': 'text/html',
        },
        'multi_token_match_align_labels': {
            'annotator_id': 'diffeo',
            'tagger_id': 'basis',
        },
        'from_local_files': {
            'encoding': 'utf-8',
        },
        'to_local_chunks': {
            'output_type': 'otherdir',
            'compress': False,
        },
    }
}


def xpath_config():
    return copy.deepcopy(default_config)


def xpath_document_file(path_in, path_out):
    opath, oname = path.dirname(path_out), path.basename(path_out)
    if oname.endswith('.sc'):
        oname = oname[:-3]

    config = xpath_config()
    config['streamcorpus_pipeline']['to_local_chunks']['output_path'] = opath
    config['streamcorpus_pipeline']['to_local_chunks']['output_name'] = oname

    with yakonfig.defaulted_config([dblogger, streamcorpus_pipeline],
                                   config=config):
        stages = PipelineStages()
        pf = PipelineFactory(stages)
        p = pf(yakonfig.get_global_config('streamcorpus_pipeline'))
        p.run(path_in)


def main():
    p = argparse.ArgumentParser(
        description='Ingest a document, compute Xpaths and output '
                    'stream item.')
    p.add_argument('inp')
    p.add_argument('out')
    args = p.parse_args()

    xpath_document_file(args.inp, args.out)
