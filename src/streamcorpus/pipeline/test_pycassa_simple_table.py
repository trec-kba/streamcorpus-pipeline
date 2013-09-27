
import pytest
import random
import getpass
import hashlib
import logging
from _pycassa_simple_table import Cassa

def mk(s):
    return hashlib.md5(str(s)).hexdigest()

namespace = 'test_' + getpass.getuser().replace('-', '_')


import config
_conf = config.get_config()
# If available, load and merge in 'test.yaml' config
_test_conf, _ = config.path_load_config(filename='configs/test.yaml')
if _test_conf is not None:
    _conf = config.deep_update(_conf, _test_conf)


if 'storage_addresses' not in _conf:
    logging.warn('not running pycassa tests without "storage_addresses" configure. You may add it ot configs/test.yaml')


#@pytest.mark.xfail # pylint: disable=E1101
@pytest.mark.skipif('_conf.get("storage_addresses", None) is None')
def test_ranges():
    config = _conf

    c = Cassa(namespace, server_list=config['storage_addresses'])
    c.delete_namespace()

    c = Cassa(namespace, server_list=config['storage_addresses'])

    keys = []
    for i in range(2**8):
        keys.append(mk(i))
    keys.sort()
    map(c.put_available, keys)

    #for row in c.pool.execute_cql3('SELECT * FROM available;'):
    #    print row

    #print list(c._available.get_range(start=keys[0], finish=keys[2**7 - 1]))
    #print list(c._available.get_range(start=keys[2**7], finish=keys[2**8-1]))
    '''
    import cql
    conn = cql.connect('localhost')
    cursor = conn.cursor()
    cursor.execute('USE test;')
    cursor.execute('CREATE TABLE available (k int PRIMARY KEY, v1 int, v2 int);')
    cursor.execute('INSERT INTO available (k, v1, v2) VALUES (0, 10, 10;')
    cursor.execute('INSERT INTO available (k, v1, v2) VALUES (1, 10, 10;')
    cursor.execute('INSERT INTO available (k, v1, v2) VALUES (2, 10, 10;')
    cursor.execute('INSERT INTO available (k, v1, v2) VALUES (3, 10, 10;')
    cursor.execute('SELECT RowKey FROM available WHERE token(RowKey) < token(2);')
    for row in cursor:
        print row

    cursor.close()
    conn.close()
    '''
    c.delete_namespace()


@pytest.mark.skipif('_conf.get("storage_addresses", None) is None')
def test_tasks():
    config = _conf

    c = Cassa(namespace, server_list=config['storage_addresses'])
    c.delete_namespace()

    c = Cassa(namespace, server_list=config['storage_addresses'])

    key = hashlib.md5(namespace).hexdigest()
    c.put_task(key, dict(test='hi'))
    for t in c.tasks():
        print t

    c.delete_namespace()

@pytest.mark.xfail
def test_available():
    config = _conf

    c = Cassa(namespace, server_list=config['storage_addresses'])
    c.delete_namespace()

    c = Cassa(namespace, server_list=config['storage_addresses'])

    key = hashlib.md5(namespace).hexdigest()
    c.put_available(key)
    ava = c.get_random_available()
    assert ava == key

    c.pop_available(ava)

    c.delete_namespace()


@pytest.mark.skipif('_conf.get("storage_addresses", None) is None')
def test_pop_available():
    config = _conf

    c = Cassa(namespace, server_list=config['storage_addresses'])
    c.delete_namespace()

    c = Cassa(namespace, server_list=config['storage_addresses'])
    
    key = hashlib.md5(namespace).hexdigest()
    c.pop_available(key)

    c.delete_namespace()


@pytest.mark.skipif('_conf.get("storage_addresses", None) is None')
def test_lengths():
    config = _conf

    c = Cassa(namespace, server_list=config['storage_addresses'])
    c.delete_namespace()

    c = Cassa(namespace, server_list=config['storage_addresses'])

    for i in range(10):
        key = hashlib.md5('test %.10f' % random.random()).hexdigest()
        c.put_task(key, dict(test='hi'))

    assert 10 == c.num_tasks()

    c.delete_namespace()


@pytest.mark.skipif('_conf.get("storage_addresses", None) is None')
def test_more_available_than():
    config = _conf

    c = Cassa(namespace, server_list=config['storage_addresses'])
    c.delete_namespace()

    c = Cassa(namespace, server_list=config['storage_addresses'])

    for i in range(10):
        key = hashlib.md5('test %.10f' % random.random()).hexdigest()
        c.put_available(key)

    assert c.num_available() == 10

    c.delete_namespace()


@pytest.mark.skipif('_conf.get("storage_addresses", None) is None')
def test_available_iter():
    config = _conf

    c = Cassa(namespace, server_list=config['storage_addresses'])
    c.delete_namespace()

    c = Cassa(namespace, server_list=config['storage_addresses'])

    for i in range(10):
        key = hashlib.md5('test %.10f' % random.random()).hexdigest()
        c.put_available(key)

    ## check that the last key comes back to us
    assert c.pop_available(key)

    c.delete_namespace()

