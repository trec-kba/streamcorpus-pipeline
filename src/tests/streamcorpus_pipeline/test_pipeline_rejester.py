import getpass
import logging
import multiprocessing
import os
import pytest
import time
import yaml

import streamcorpus
from streamcorpus_pipeline._logging import configure_logger
from streamcorpus_pipeline._rejester import rejester_run_function
from rejester.workers import run_worker, MultiWorker

logger = configure_logger(__name__)

@pytest.fixture(scope="function") # (scope="module")
def root_path(request):
    """The root path of the streamcorpus-pipeline tree."""
    here = os.path.dirname(__file__)
    # note assertions about source tree layout
    # we are in streamcorpus-pipeline/src/tests/streamcorpus_pipeline
    tests = os.path.dirname(here)
    src = os.path.dirname(tests)
    scp = os.path.dirname(src)
    return scp

def job_status(master, name):
    """Get a quick summary of the status of 'name' as a (printable)
    string."""
    parts = []
    a = master.num_available(name)
    if a > 0: parts.append("{0} available".format(a))
    p = master.num_pending(name)
    if p > 0: parts.append("{0} pending".format(p))
    f = master.num_finished(name)
    if f > 0: parts.append("{0} finished".format(f))
    z = master.num_failed(name)
    if z > 0: parts.append("{0} failed".format(f))
    if len(parts) == 0: parts.append("no status")
    return ', '.join(parts)

def jobs_status(master, jobs):
    """Get a quick summary of the statuses of all of 'jobs' as a
    (printable) string."""
    return '; '.join(['{0}: {1}'.format(j, job_status(master, j))
                      for j in jobs])

def test_rejester_john_smith_simple(root_path, task_master):

    os.chdir(root_path)
    configs = [ 'john-smith', 'john-smith-with-labels-from-tsv' ]
    inputs = [ 'data/john-smith/original' ]

    # set up the job specs:
    work_specs = {}
    units = {}
    for c in configs:
        fn = os.path.join(root_path, 'configs', c + '.yaml')
        with open(fn, 'r') as f:
            work_specs[c] = {
                'name': c,
                'desc': 'test_pipeline_rejester for {0}'.format(c),
                'min_gb': 0,
                'config': yaml.load(f),
                'module': 'streamcorpus_pipeline._rejester',
                'run_function': 'rejester_run_function',
                'terminate_function': 'rejester_terminate_function'
            }
            units[c] = {
                os.path.join(root_path, i): dict(start_chunk_time=0)
                for i in inputs
            }
            task_master.update_bundle(work_specs[c], units[c])

    # kick everything off
    task_master.set_mode(task_master.RUN)
    p = multiprocessing.Process(target=run_worker,
                                args=(MultiWorker, task_master.registry.config))
    p.start()
    try:
        start_time = time.time()
        end_time = start_time + 10
        last_status = None
        while time.time() < end_time:
            # log a status message with our job progress
            status = jobs_status(task_master, configs)
            if status != last_status:
                logger.info(status)
                last_status = status

            # stop if we've finished all of the jobs
            done = all([task_master.num_available(c) == 0 and
                        task_master.num_pending(c) == 0
                        for c in configs])
            if done:
                logger.info("all jobs done, stopping")
                break

            # if the subprocess died, then there are no more workers,
            # and there's no point in continuing this loop
            # (we expect the workers to go back to "idle" state)
            if not p.is_alive():
                raise Exception("rejester workers stopped")

            # otherwise wait for a short bit and reloop
            time.sleep(0.1)
    except Exception, e:
        logger.error("something went wrong", exc_info=True)
    finally:
        logger.info("shutting down master")
        task_master.set_mode(task_master.TERMINATE)
        logger.info("waiting for workers to stop")
        p.join(5.0)
        if p.is_alive():
            logger.warn("workers did not stop in a timely fashion")
            p.terminate()
            p.join()
        else:
            logger.info("workers stopped cleanly")

    assert p.exitcode != -15, \
            "had to kill off workers (or someone else killed them)"
    assert p.exitcode == 0

    # fetch back all of the work units and verify they succeeded
    for c in configs:
        for i in inputs:
            ii = os.path.join(root_path, i)
            assert units[c][ii].get('traceback') is None
        assert task_master.num_failed(c) == 0
        assert task_master.num_finished(c) == len(inputs)

    # if we've gotten *this* far then we should be able to compare
    # that the tests produced sufficiently similar outputs...
    # which is to say that they produce the same set of stream_id
    chunk_configs = [
        c['config']['streamcorpus_pipeline']['to_local_chunks']
        for c in work_specs.values()
        ]
    outputs = [os.path.join(c['output_path'],
                            c['output_name'] % { 'first': '0' } + '.sc')
               for c in chunk_configs]
    def read_stream_id(fn):
        ids = set()
        for si in streamcorpus.Chunk(path=fn, mode='rb',
                                     message=streamcorpus.StreamItem_v0_3_0):
            ids.add(si.stream_id)
        return ids
    stream_ids = [read_stream_id(fn) for fn in outputs]
    assert len(stream_ids) == 2
    assert stream_ids[0] == stream_ids[1]
