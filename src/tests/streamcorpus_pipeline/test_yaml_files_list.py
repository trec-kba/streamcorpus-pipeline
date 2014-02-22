# import time
# import errno
import os
from streamcorpus_pipeline._yaml_files_list import yaml_files_list

def test_parse_file():
    yfl = yaml_files_list({})
    input_file = os.path.join(os.path.dirname(__file__), '../../../data/john-smith/ground-truth.yaml')
    cnt = 0
    for si in yfl(input_file):
        assert si.stream_id
        assert si.body.raw
        cnt += 1
    assert cnt == 197

def test_parse_mentions():
    raw_mentions = ['John Smith', {'name': 'John Smith'}, {'ip_address': '10.0.0.1'}]
    yfl = yaml_files_list({})
    mentions = yfl._parse_mentions(raw_mentions)

    assert len(mentions) == len(raw_mentions)
    assert mentions == [('name', 'John Smith'), ('name', 'John Smith'), ('ip_address', '10.0.0.1')]
