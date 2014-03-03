from __future__ import absolute_import
from cStringIO import StringIO
import logging
import os
import signal
import sys
import time

import gevent
import pytest
import yaml

import streamcorpus_pipeline
from streamcorpus_pipeline import Pipeline
from streamcorpus_pipeline.tests._test_data import get_test_chunk_path, \
    get_test_chunk, \
    get_test_v0_3_0_chunk_path, \
    get_test_v0_3_0_chunk_tagged_by_serif_path
import yakonfig

logger = logging.getLogger(__name__)

class SuccessfulExit(Exception):
    pass

def test_pipeline(request, monkeypatch):
    def mockexit(status=0):
        logger.debug('sys.exit({})'.format(status))
        raise SuccessfulExit()
    monkeypatch.setattr(sys, 'exit', mockexit)
    filename=str(request.fspath.dirpath('test_dedup_chunk_counts.yaml'))
    with yakonfig.defaulted_config([streamcorpus_pipeline], filename=filename):
        ## config says read from stdin, so make that have what we want
        stdin = sys.stdin
        sys.stdin = StringIO(get_test_chunk_path())

        ## run the pipeline
        p = Pipeline()

        from streamcorpus_pipeline.run import SimpleWorkUnit
        work_unit = SimpleWorkUnit('long string indicating source of text')
        work_unit.data['start_chunk_time'] = time.time()
        work_unit.data['start_count'] = 0
        g = gevent.spawn(p._process_task, work_unit)

        gevent.sleep(5)

        with pytest.raises(SuccessfulExit):  # pylint: disable=E1101
            p.shutdown(sig=signal.SIGTERM)

        logger.debug('now joining...')
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
