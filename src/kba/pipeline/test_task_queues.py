import time
from _stages import _init_stage
from operator import itemgetter

def test_stdin():
    stdin = _init_stage('stdin', {})
    

def test_zk():
    config = dict(
        zookeeper_address = 'localhost:2181',
        namespace = 'kba-pipeline-task-queue-test',
        zookeeper_timeout = 120,
        config_hash = '',
        config_json = '',
        )

    test_data = set(['a', 'b', 'c', 'd'])

    tq1 = _init_stage('zookeeper', config)
    tq1.delete_all()

    tq1 = _init_stage('zookeeper', config)

    map(tq1.push, test_data)
    tq1.set_mode(tq1.FINISH)

    tq2 = _init_stage('zookeeper', config)
    received_data = set(map(itemgetter(1), tq2))

    assert received_data == test_data
        
    assert tq2._len('available') == 0
    assert tq2._len('pending') == 0

def test_zk_commit():
    config = dict(
        zookeeper_address = 'localhost:2181',
        namespace = 'kba-pipeline-task-queue-test',
        zookeeper_timeout = 120,
        config_hash = '',
        config_json = '',
        )

    test_data = set(['a', 'b', 'c', 'd'])

    tq1 = _init_stage('zookeeper', config)
    tq1.delete_all()
    tq1 = _init_stage('zookeeper', config)

    map(tq1.push, test_data)
    tq1.set_mode(tq1.FINISH)

    for t in tq1:
        tq1.commit('finished it with foo')

def test_zk_partial_commit():
    config = dict(
        zookeeper_address = 'localhost:2181',
        namespace = 'kba-pipeline-task-queue-test',
        zookeeper_timeout = 120,
        finish_ramp_down_fraction = 0.0,
        config_hash = '',
        config_json = '',
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
    received = set([(start_count, task_string, '{}')
                 for start_count, task_string,  data in tq1])
    assert expected == received
