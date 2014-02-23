from __future__ import absolute_import
import errno
import time

import pytest

import streamcorpus_pipeline
from streamcorpus_pipeline._local_storage import from_local_chunks
import yakonfig

def test_max_retries():
    config_yaml = """
streamcorpus_pipeline:
    from_local_chunks:
        max_retries: 5
        max_backoff: 100
        streamcorpus_version: v0_2_0
"""
    with yakonfig.defaulted_config([streamcorpus_pipeline], yaml=config_yaml,
                                   validate=False):
        flc = from_local_chunks()
        start_time = time.time()
        with pytest.raises(IOError) as excinfo:  # pylint: disable=E1101
            flc('no-such-path')
        elapsed = time.time() - start_time
        assert elapsed > 0.1 * 2**4
        assert excinfo.value.errno == errno.ENOENT

def test_max_backoff():
    config_yaml = """
streamcorpus_pipeline:
    from_local_chunks:
        max_retries: 5
        max_backoff: 2
        streamcorpus_version: v0_2_0
"""
    with yakonfig.defaulted_config([streamcorpus_pipeline], yaml=config_yaml,
                                   validate=False):
        flc = from_local_chunks()
        start_time = time.time()
        with pytest.raises(IOError) as excinfo:  # pylint: disable=E1101
            flc('no-such-path')
        elapsed = time.time() - start_time
        assert elapsed < 4
        assert excinfo.value.errno == errno.ENOENT
