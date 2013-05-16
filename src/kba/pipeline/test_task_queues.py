import time
import pytest
from _stages import _init_stage
from operator import itemgetter

def test_stdin():
    stdin = _init_stage('stdin', {})
    
from config import get_config

@pytest.mark.skipif('True')  # pylint: disable=E1101
def test_zk():
    config = get_config(
        namespace = 'kba_pipeline_task_queue_test',
        config_hash = '',
        config_json = '',
        min_workers = 1,
        )

    test_data = set(['a', 'b', 'c', 'd'])

    tq1 = _init_stage('zookeeper', config)
    tq1.delete_all()

    tq1 = _init_stage('zookeeper', config)

    map(tq1.push, test_data)
    tq1.set_mode(tq1.FINISH)

    tq2 = _init_stage('zookeeper', config)
    received_data = set()
    for end_count, i_str, data in tq2:
        assert data['state'] == 'pending'
        tq2.commit()
        received_data.add(i_str)

    assert received_data == test_data
        
    assert tq2._len('available') == 0
    assert tq2._len('pending') == 0

    tq2.delete_all()

@pytest.mark.skipif('True')  # pylint: disable=E1101
def test_zk_commit():
    config = get_config(
        namespace = 'kba_pipeline_task_queue_test',
        config_hash = '',
        config_json = '',
        min_workers = 1,
        )

    test_data = set(['a', 'b', 'c', 'd'])

    tq1 = _init_stage('zookeeper', config)
    tq1.delete_all()
    tq1 = _init_stage('zookeeper', config)

    map(tq1.push, test_data)
    tq1.set_mode(tq1.FINISH)

    for t in tq1:
        tq1.commit('finished it with foo')

    tq1.delete_all()

@pytest.mark.skipif('True')  # pylint: disable=E1101
def test_zk_partial_commit():
    config = get_config(
        namespace = 'kba_pipeline_task_queue_test',
        config_hash = '',
        config_json = '',
        min_workers = 1,
        )

    test_data = set(['a', 'b', 'c', 'd'])

    ## clear it all
    tq1 = _init_stage('zookeeper', config)
    tq1.delete_all()

    ## populate queue
    tq1 = _init_stage('zookeeper', config)
    map(tq1.push, test_data)
    tq1.set_mode(tq1.FINISH)

    ## get a new client
    tq1 = _init_stage('zookeeper', config)

    tasks = iter(tq1)
    start, t0, data = tasks.next()
    assert start == 0
    tq1.partial_commit(0, 10, ['some path'])
    tq1.partial_commit(10, 20, ['some path'])
    tq1.partial_commit(20, 30, ['some path'])

    time.sleep(1)

    ## get a new client
    tq1 = _init_stage('zookeeper', config)
    tq1.reset_pending()

    ## get a new client
    tq1 = _init_stage('zookeeper', config)
    expected = set( [(30, t0, '{}')] )
    for letter in test_data:
        if letter != t0:
            expected.add( (0, letter, '{}') )

    received = set()
    for start_count, task_str, data in tq1:
        tq1.commit()
        received.add((start_count, task_str, '{}'))

    assert expected == received

    tq1.delete_all()
