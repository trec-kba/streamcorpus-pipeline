import time
import errno
import pytest
from _local_storage import from_local_chunks

def test_max_retries():
    config = dict(
        max_retries = 5,
        max_backoff = 100,
        )
    flc = from_local_chunks(config)
    start_time = time.time()
    with pytest.raises(IOError) as excinfo:
        flc('no-such-path')
    elapsed = time.time() - start_time
    assert elapsed > 0.1 * 2**4
    assert excinfo.value.errno == errno.ENOENT

def test_max_backoff():
    config = dict(
        max_retries = 5,
        max_backoff = 2,
        )
    flc = from_local_chunks(config)
    start_time = time.time()
    with pytest.raises(IOError) as excinfo:
        flc('no-such-path')
    elapsed = time.time() - start_time
    assert elapsed < 4
    assert excinfo.value.errno == errno.ENOENT