# import time
# import errno
import pytest
import os
from streamcorpus_pipeline._yaml_files_list import yaml_files_list

def test_parse_file():
    yfl = yaml_files_list({})
    input_file = os.path.join(os.path.dirname(__file__), '../../../data/john-smith/ground-truth.yaml')

    for si in yfl(input_file):
        assert si.stream_id
        assert si.body.raw
