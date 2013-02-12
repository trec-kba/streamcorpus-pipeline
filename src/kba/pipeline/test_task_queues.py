from _stages import _init_stage
from operator import itemgetter

def test_stdin():
    stdin = _init_stage('stdin', {})
    

def test_zk():
    config = dict(
        zookeeper_address = 'localhost:2181',
        namespace = 'kba-pipeline-task-queue',
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
        namespace = 'kba-pipeline-task-queue',
        )

    test_data = set(['a', 'b', 'c', 'd'])

    tq1 = _init_stage('zookeeper', config)
    tq1.delete_all()
    tq1 = _init_stage('zookeeper', config)

    map(tq1.push, test_data)
    tq1.set_mode(tq1.FINISH)

    for t in tq1:
        tq1.commit('finished it with foo')
