from _stages import _init_stage

def test_stdin():
    stdin = _init_stage('stdin', {})
    

def test_zk():
    config = dict(
        zookeeper_address = 'localhost:2181',
        namespace = 'kba-pipeline-task-queue',
        )

    test_data = set(['a', 'b', 'c', 'd'])

    config['delete_all'] = True
    tq1 = _init_stage('zookeeper', config)
    map(tq1.push, test_data)
    tq1.set_mode(tq1.FINISH)

    config.pop('delete_all')
    tq2 = _init_stage('zookeeper', config)
    received_data = set(tq2)

    assert received_data == test_data
        
