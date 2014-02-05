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

def log(m):
    sys.stderr.write(m)
    sys.stderr.write('\n')
    sys.stderr.flush()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(__doc__)
    parser.add_argument('base_path', help='path to "original" John Smith corpus directory containing a subdirectory for each entity')
    args = parser.parse_args()

    metadata = dict()
    #metadata['abs_url_base'] = base_path
    metadata['source'] = 'bagga-and-baldwin'
    metadata['annotator_id'] = 'bagga-and-baldwin'
    #metadata['root_path'] = base_path

    entries = []
    for fname in os.listdir(args.base_path):
        fpath = os.path.join(args.base_path, fname)
        if os.path.isdir(fpath):
            entry = {
                'target_id': fname,
                'doc_path': os.path.abspath(fpath + '/'),
                'mentions': ['John Smith'],
                }
            entries.append(entry)
    metadata['entries'] = entries
    yaml.dump(metadata, stream=sys.stdout)
