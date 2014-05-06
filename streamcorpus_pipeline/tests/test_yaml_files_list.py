from __future__ import absolute_import
import os

from streamcorpus_pipeline._yaml_files_list import yaml_files_list

def test_parse_file(test_data_dir):
    yfl = yaml_files_list(config={})
    input_file = os.path.join(test_data_dir, 'john-smith/ground-truth.yaml')
    os.chdir(os.path.join(test_data_dir, 'john-smith/'))
    cnt = 0
    for si in yfl(input_file):
        assert si.stream_id
        assert si.body.raw
        cnt += 1
    assert cnt == 197

def test_parse_mentions():
    yfl = yaml_files_list(config={})
    raw_mentions = ['John Smith', {'name': 'John Smith'}, {'ip_address': '10.0.0.1'}]
    mentions = yfl._parse_slots(raw_mentions)

    assert len(mentions) == len(raw_mentions)
    assert mentions == [('name', 'John Smith'), ('name', 'John Smith'), ('ip_address', '10.0.0.1')]
