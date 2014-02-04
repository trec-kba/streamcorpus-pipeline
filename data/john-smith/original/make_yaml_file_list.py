#!/usr/bin/env python

"""
Generate file spec for 'yaml_files_list' reader.

python make_yaml_file_list.py  > john_smith_files.yaml

streamcorpus_pipeline ../../../configs/john-smith-serif.yaml  -i john_smith_files.yaml 
"""

import os
import sys

import yaml


if __name__ == '__main__':
    base_path = os.getcwd()
    metadata = dict()
    #metadata['abs_url_base'] = base_path
    metadata['source'] = 'bagga-and-baldwin'
    metadata['annotator_id'] = 'bagga-and-baldwin'
    #metadata['root_path'] = base_path

    entries = []
    for fname in os.listdir(base_path):
        if fname[0] == '.':
            continue
        if os.path.isdir(fname):
            entry = {
                'target_id': fname,
                'doc_path': fname + '/',
                'mentions': ['John Smith'],
                }
            entries.append(entry)
    metadata['entries'] = entries
    yaml.dump(metadata, stream=sys.stdout)
