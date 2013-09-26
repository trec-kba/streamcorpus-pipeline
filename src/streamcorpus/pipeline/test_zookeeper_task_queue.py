import os
import pytest
import random

from _task_queues import ZookeeperTaskQueue
from config import get_config

def _test_config():
    return get_config(
        zookeeper = dict(
            namespace = 'random_available_task_test',
            config_hash = '',
            config_json = '',
            min_workers = 1,
        )
    )

# TODO: needs a zookeeper server configured to run against. detect that?
@pytest.mark.skipif('True')
def test_available_path():
    config = _test_config()
   
    zktq = ZookeeperTaskQueue(config)
    zktq.delete_all()
    zktq.init_all()
    zktq._available_levels = 1
    assert os.path.join('available_path_test', 
                        'available', 
                        '12', 
                        '123456789') == zktq._available_path('123456789')
    zktq._available_levels = 2
    assert os.path.join('available_path_test', 
                        'available', 
                        '12', 
                        '34', 
                        '123456789') == zktq._available_path('123456789')

# TODO: needs a zookeeper server configured to run against. detect that?
@pytest.mark.skipif('True')
def test_put_pop_available():
    config = _test_config()
 
    zktq = ZookeeperTaskQueue(config)
    zktq.delete_all()
    zktq.init_all()
    zktq._put_available('12345678')
    zktq._pop_available('12345678')

# TODO: needs a zookeeper server configured to run against. detect that?
@pytest.mark.skipif('True')
def test_get_random_available_task():
    config = _test_config()
    
    zktq = ZookeeperTaskQueue(config)
    zktq.delete_all()
    zktq.init_all()
    task_keys = set()
    for x in xrange(100): 
        key = str(random.randint(0,999999999))
        task_keys.add(key)
        zktq._put_available(key)
    random_key = zktq._random_available_task()
    assert random_key in task_keys

# TODO: needs a zookeeper server configured to run against. detect that?
@pytest.mark.skipif('True')
def test_random_available_task():
    config = _test_config()
    
    zktq = ZookeeperTaskQueue(config)
    zktq.delete_all()
    zktq.init_all()
    task_keys = set()

    ## push 5 random keys into the available list
    for x in xrange(5): 
        key = str(random.randint(0,9999))
        task_keys.add(key)
        zktq._put_available(key)

    ## pop them all out 
    for x in xrange(5):
        ## Get random key
        random_key = zktq._random_available_task()
        while not random_key:
            random_key = zktq._random_available_task()

        assert random_key in task_keys
        task_keys.discard(random_key)
        zktq._pop_available(random_key)

    assert zktq._num_available() == 0

## must fix this before using ZookeeperTaskQueue
#@pytest.mark.xfail # pylint: disable=E1101
# TODO: needs a zookeeper server configured to run against. detect that?
@pytest.mark.skipif('True')
def test_num_available():
    config = _test_config()
    
    zktq = ZookeeperTaskQueue(config)
    zktq.delete_all()
    zktq.init_all()
    task_keys = set()

    zktq._put_available('10000')
    assert zktq._num_available() == 256 ** 2

    random_key = zktq._random_available_task()
    assert zktq._num_available() == 256 ** 2

    zktq._put_available('10001')
    zktq._put_available('10002')
    random_key = zktq._random_available_task()
    assert zktq._num_available() == 256 ** 2 * 3



