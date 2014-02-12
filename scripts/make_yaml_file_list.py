#!/usr/bin/env python

"""
Generate file spec for 'yaml_files_list' reader.
To run from streamcorpus-pipeline dir:

python scripts/make_yaml_file_list.py --datadir=data/john-smith/original --out=john_smith_files.yaml

streamcorpus_pipeline configs/john-smith-serif.yaml -i john_smith_files.yaml 
"""

import argparse
import os
import sys

import yaml


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--datadir', default=None, dest='datadir')
    ap.add_argument('arg', nargs='?', default=None)
    ap.add_argument('-o', '--out', default=None)
    ap.add_argument('--abs-url-is-path', action='store_true', default=False)
    args = ap.parse_args()

    if args.out and (args.out != '-'):
        outf = open(args.out, 'wb')
    else:
        outf = sys.stdout

    base_path = args.datadir or args.arg
    if base_path:
        base_path = os.path.abspath(base_path)
    metadata = dict()
    if base_path:
        metadata['root_path'] = base_path
    if base_path and args.abs_url_is_path:
        # for debugging, use local filesystem absolute pathe
        metadata['abs_url_base'] = base_path
    else:
        # For the purposes of this analysis, the trec-kba project shall be
        # considered to be the canonical absolute url source for the "John
        # Smith" data.
        metadata['abs_url_base'] = 'https://raw2.github.com/trec-kba/streamcorpus-pipeline/master/data/john-smith/original'
    metadata['source'] = 'bagga-and-baldwin'
    metadata['annotator_id'] = 'bagga-and-baldwin'

    entries = []
    if not base_path:
        base_path = os.path.abspath(os.getcwd())
    for fname in os.listdir(base_path):
        if fname[0] == '.':
            continue
        fpath = os.path.join(base_path, fname)
        if os.path.isdir(fpath):
            entry = {
                'target_id': fname,
                'doc_path': fname + '/',
                'mentions': ['John', 'Smith'],
                }
            entries.append(entry)
    metadata['entries'] = entries
    yaml.dump(metadata, stream=outf)


if __name__ == '__main__':
    main()
