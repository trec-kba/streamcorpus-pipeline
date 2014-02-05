#!/usr/bin/env python

"""
Generate file spec for 'yaml_files_list' reader.

python make_yaml_file_list.py  > john_smith_files.yaml

streamcorpus_pipeline ../../../configs/john-smith-serif.yaml  -i john_smith_files.yaml 
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
        #metadata['abs_url_base'] = base_path
        metadata['root_path'] = base_path
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
                'mentions': ['John Smith'],
                }
            entries.append(entry)
    metadata['entries'] = entries
    yaml.dump(metadata, stream=outf)


if __name__ == '__main__':
    main()
