# This is really a system test, not a unit test.  Still, it looks like
# a pretty good system test...except that (a) it lightly assumes the
# removed coordinate.workers.MultiWorker implementation, and (b) it
# documents itself as being subtly broken.  This should at some point get
# resurrected.

import logging
import multiprocessing
import os
import time
import yaml

import pytest

import streamcorpus
from coordinate.workers import run_worker, MultiWorker

logger = logging.getLogger(__name__)
from coordinate.tests.test_job_client import task_master, job_queue

def job_status(master, name):
    """Get a quick summary of the status of 'name' as a (printable)
    string."""
    parts = []
    st = master.status(name)
    a = st['num_available']
    if a > 0:
        parts.append("{0} available".format(a))
    p = st['num_pending']
    if p > 0:
        parts.append("{0} pending".format(p))
    f = st['num_finished']
    if f > 0:
        parts.append("{0} finished".format(f))
    z = st['num_failed']
    if z > 0:
        parts.append("{0} failed".format(z))
    if len(parts) == 0:
        parts.append("no status")
    return ', '.join(parts)

def jobs_status(master, jobs):
    """Get a quick summary of the statuses of all of 'jobs' as a
    (printable) string."""
    return '; '.join(['{0}: {1}'.format(j, job_status(master, j))
                      for j in jobs])

#@pytest.mark.xfail
def test_coordinate_john_smith_simple(task_master, test_data_dir, tmpdir):
    pytest.skip('test is subtly broken, but could maybe yet be fixed')
    configs = [ 'john-smith']
    inputs = [ 'john-smith/original' ]

    # set up the job specs:
    work_specs = {}
    units = {}
    config_root = os.path.join(os.path.dirname(__file__), 'configs')
    for c in configs:
        fn = os.path.join(config_root, c + '.yaml')
        with open(fn, 'r') as f:
            config = yaml.load(f)
        assert 'streamcorpus_pipeline' in config
        root = os.path.dirname(__file__)
        config['streamcorpus_pipeline']['root_path'] = root
        assert 'writers' in config['streamcorpus_pipeline']
        assert 'to_local_chunks' in config['streamcorpus_pipeline']['writers']
        assert 'to_local_chunks' in config['streamcorpus_pipeline']
        tlc = config['streamcorpus_pipeline']['to_local_chunks']
        tlc['output_type'] = 'otherdir'
        tlc['output_path'] = str(tmpdir)
        work_specs[c] = {
            'name': c,
            'desc': 'test_pipeline_coordinate for {0}'.format(c),
            'min_gb': 0,
            'config': config,
            'module': 'streamcorpus_pipeline._coordinate',
            'run_function': 'coordinate_run_function',
            'terminate_function': 'coordinate_terminate_function'
        }
        units[c] = dict([
            (os.path.join(test_data_dir, i), dict(start_chunk_time=0))
            for i in inputs])
        task_master.update_bundle(work_specs[c], units[c])

    # kick everything off
    task_master.set_mode(task_master.RUN)
    p = multiprocessing.Process(target=run_worker,
                                args=(MultiWorker, task_master.config))
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
            st = task_master.status(c)
            done = all([st['num_available'] == 0 and
                        st['num_pending'] == 0
                        for c in configs])
            if done:
                logger.info("all jobs done, stopping")
                break

            # if the subprocess died, then there are no more workers,
            # and there's no point in continuing this loop
            # (we expect the workers to go back to "idle" state)
            if not p.is_alive():
                raise Exception("coordinate workers stopped")

            # otherwise wait for a short bit and reloop
            time.sleep(0.1)
    except Exception:
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
            ii = os.path.join(test_data_dir, i)
            assert task_master.inspect_work_unit(c, ii).get('traceback') is None
        st = task_master.status(c)
        assert st['num_failed'] == 0
        assert st['num_finished'] == len(inputs)

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
    assert len(stream_ids) == len(outputs)
