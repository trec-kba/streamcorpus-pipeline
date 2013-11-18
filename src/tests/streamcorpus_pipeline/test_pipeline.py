import os
import sys
import yaml
import time
import pytest
import gevent
import signal
from cStringIO import StringIO
from streamcorpus_pipeline import Pipeline
from streamcorpus_pipeline._logging import logger

from _test_data import get_test_chunk_path, get_test_chunk, \
    get_test_v0_3_0_chunk_path, \
    get_test_v0_3_0_chunk_tagged_by_serif_path

class SuccessfulExit(Exception):
    pass

def log(msg):
    print msg
    sys.stdout.flush()

def test_pipeline(monkeypatch):
    def mockexit(status=0):
        log( ' sys.exit(%d) ' % status )
        raise SuccessfulExit()
    monkeypatch.setattr(sys, 'exit', mockexit)
    path = os.path.dirname(__file__)
    config = yaml.load(open(os.path.join(path, 'test_dedup_chunk_counts.yaml')))

    ## config says read from stdin, so make that have what we want
    stdin = sys.stdin
    sys.stdin = StringIO(get_test_chunk_path())

    ## run the pipeline
    p = Pipeline( config )

    from streamcorpus_pipeline.run import SimpleWorkUnit
    work_unit = SimpleWorkUnit('long string indicating source of text')
    work_unit.data['start_chunk_time'] = time.time()
    work_unit.data['start_count'] = 0
    g = gevent.spawn(p._process_task, work_unit)

    gevent.sleep(5)

    with pytest.raises(SuccessfulExit):  # pylint: disable=E1101
        p.shutdown(sig=signal.SIGTERM)

    log( 'now joining...' )
    timeout = gevent.Timeout(1)
    g.join(timeout=timeout)


@pytest.mark.skipif('True')  # pylint: disable=E1101
def test_post_batch_incremental_stage():
    path = os.path.dirname(__file__)
    config = yaml.load(open(os.path.join(path, 'test_post_batch_incremental.yaml')))

    ## config says read from stdin, so make that have what we want
    stdin = sys.stdin
    sys.stdin = StringIO(get_test_chunk_path())

    ## run the pipeline
    p = Pipeline( config )
    p.run()

@pytest.mark.skipif('True')  # pylint: disable=E1101
def test_align_serif_stage():
    path = os.path.dirname(__file__)
    config = yaml.load(open(os.path.join(path, 'test_align_serif_stage.yaml')))

    ## config says read from stdin, so make that have what we want
    stdin = sys.stdin
    sys.stdin = StringIO(get_test_v0_3_0_chunk_tagged_by_serif_path())

    ## run the pipeline
    p = Pipeline( config )
    p.run()
