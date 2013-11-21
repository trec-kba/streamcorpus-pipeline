import pytest
import rejester

## 1KB sized work_spec config
work_spec = dict(
    name = 'tbundle',
    desc = 'a test work bundle',
    min_gb = 8,
    config = dict(many=' ' * 2**10, params=''),
    module = 'tests.rejester.test_workers',
    run_function = 'work_program',
    terminate_function = 'work_program',
)


@pytest.mark.skipif('True')
def test_rejester():

    work_units = {s3_url: dict(start_count=0) 
                  for s3_url in test_s3_urls}

    subprocess.call('rejester')

    task_master.update_bundle(work_spec, work_units)

    task_master.set_mode(task_master.RUN)

    p = multiprocessing.Process(target=run_worker, 
                                args=(MultiWorker, task_master.registry.config))
    num_workers = multiprocessing.cpu_count()
    logger.critical('expecting num_workers=%d', num_workers)

    start = time.time()
    max_test_time = 60
    finished_cleanly = False
    already_set_idle = False
    already_set_terminate = False
    p.start()

    while time.time() - start < max_test_time:

        modes = task_master.mode_counts()
        logger.critical(modes)

        if modes[task_master.RUN] >= num_workers:
            task_master.idle_all_workers()
            already_set_idle = True

        if modes[task_master.IDLE] >= 0 and already_set_idle:
            logger.critical('setting mode to TERMINATE')
            task_master.set_mode(task_master.TERMINATE)
            already_set_terminate = True
        
        if p.exitcode is not None:
            assert already_set_idle
            assert already_set_terminate
            if p.exitcode == 0:
                finished_cleanly = True
                break

        time.sleep(2)

    if not finished_cleanly:
        raise Exception('timed out after %d seconds' % (time.time() - start))

    p.join()
    logger.info('finished running %d worker processes', num_workers)

    assert task_master.num_failed(work_spec['name']) == 0
    assert task_master.num_finished(work_spec['name']) >= num_workers

