from __future__ import absolute_import
import errno
import os
import time

import pytest

from streamcorpus_pipeline._local_storage import from_local_chunks, \
    from_local_files


def test_max_retries():
    flc = from_local_chunks(config={
        'max_retries': 5,
        'max_backoff': 100,
        'streamcorpus_version': 'v0_2_0',
    })
    start_time = time.time()
    with pytest.raises(IOError) as excinfo:  # pylint: disable=E1101
        flc('no-such-path')
    elapsed = time.time() - start_time
    assert elapsed > 0.1 * 2**4
    assert excinfo.value.errno == errno.ENOENT


def test_max_backoff():
    flc = from_local_chunks(config={
        'max_retries': 5,
        'max_backoff': 2,
        'streamcorpus_version': 'v0_2_0',
    })
    start_time = time.time()
    with pytest.raises(IOError) as excinfo:  # pylint: disable=E1101
        flc('no-such-path')
    elapsed = time.time() - start_time
    assert elapsed < 4
    assert excinfo.value.errno == errno.ENOENT


def test_basic_file():
    flf = from_local_files(config=from_local_files.default_config)
    sis = list(flf(__file__))
    assert len(sis) == 1
    si = sis[0]
    assert si.abs_url == 'file://' + __file__
    assert 'from __future__ import' in si.body.raw


def test_file_abspath():
    flf = from_local_files(config=from_local_files.default_config)
    i_str = os.path.relpath(__file__, os.getcwd())
    sis = list(flf(i_str))
    assert len(sis) == 1
    si = sis[0]
    assert si.abs_url == 'file://' + __file__
    assert 'from __future__ import' in si.body.raw


def test_file_urllike():
    config = dict(from_local_files.default_config)
    url_prefix = 'http://github.com/streamcorpus/streamcorpus_pipeline/'
    config['url_prefix'] = url_prefix
    config['absolute_filename'] = False
    flf = from_local_files(config)
    i_str = os.path.relpath(__file__, os.getcwd())
    sis = list(flf(i_str))
    assert len(sis) == 1
    si = sis[0]
    assert si.abs_url == url_prefix + i_str
    assert 'from __future__ import' in si.body.raw


def test_file_absurl():
    config = dict(from_local_files.default_config)
    config['abs_url'] = 'http://artifical.abs_url.com/'
    flf = from_local_files(config)
    sis = list(flf(__file__))
    assert len(sis) == 1
    si = sis[0]
    assert si.abs_url == config['abs_url']
    assert 'from __future__ import' in si.body.raw


def test_file_epoch_ticks(monkeypatch):
    class MockStat(object):
        st_mtime = 1357924680

        def __call__(self, path):
            return self

    monkeypatch.setattr(os, 'stat', MockStat())

    flf = from_local_files(config=from_local_files.default_config)
    sis = list(flf(__file__))
    assert len(sis) == 1
    si = sis[0]
    assert si.stream_time.epoch_ticks == 1357924680


def test_file_epoch_ticks_fixed():
    config = dict(from_local_files.default_config)
    config['epoch_ticks'] = 1234567890
    flf = from_local_files(config)
    sis = list(flf(__file__))
    assert len(sis) == 1
    si = sis[0]
    assert si.stream_time.epoch_ticks == config['epoch_ticks']


def test_file_epoch_ticks_now(monkeypatch):
    def mocktime():
        return 1470258369
    monkeypatch.setattr(time, 'time', mocktime)

    config = dict(from_local_files.default_config)
    config['epoch_ticks'] = 'now'
    flf = from_local_files(config)
    sis = list(flf(__file__))
    assert len(sis) == 1
    si = sis[0]
    assert si.stream_time.epoch_ticks == 1470258369
