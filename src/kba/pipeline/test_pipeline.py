import os
import sys
import yaml
import pytest
import gevent
import logging
from cStringIO import StringIO
from _pipeline import Pipeline

from _test_data import get_test_chunk_path, get_test_chunk

logger = logging.getLogger('kba')
logger.setLevel( logging.DEBUG )

ch = logging.StreamHandler()
ch.setLevel( logging.DEBUG )
formatter = logging.Formatter('%(asctime)s %(process)d %(levelname)s: %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

class SuccessfulExit(Exception):
    pass

def log(msg):
    print msg
    sys.stdout.flush()

def test_pipeline(monkeypatch):
    def mockexit():
        log( ' sys.exit called ' )
        raise SuccessfulExit()
    monkeypatch.setattr(sys, 'exit', mockexit)
    path = os.path.dirname(__file__)
    config = yaml.load(open(os.path.join(path, 'test_dedup_chunk_counts.yaml')))

    ## config says read from stdin, so make that have what we want
    stdin = sys.stdin
    sys.stdin = StringIO(get_test_chunk_path())

    ## run the pipeline
    p = Pipeline( config )
    g = gevent.spawn(p.run)

    gevent.sleep(5)

    with pytest.raises(SuccessfulExit):
        p.shutdown()

    log( 'now joining...' )
    timeout = gevent.Timeout(1)
    g.join(timeout=timeout)

