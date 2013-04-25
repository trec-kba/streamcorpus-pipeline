
import pytest
import random
import hashlib
from _pycassa_simple_table import Cassa

def mk(s):
    return hashlib.md5(str(s)).hexdigest()

from config import get_config

#@pytest.mark.xfail
def test_ranges():

    config = get_config(namespace='test')

    c = Cassa('test', server_list=config['storage_addresses'])
    c.delete_namespace()

    c = Cassa('test', server_list=config['storage_addresses'])

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

def test_tasks():
    config = get_config(namespace='test')

    c = Cassa('test', server_list=config['storage_addresses'])
    c.delete_namespace()

    c = Cassa('test', server_list=config['storage_addresses'])

    key = hashlib.md5('test').hexdigest()
    c.put_task(key, dict(test='hi'))
    for t in c.tasks:
        print t

def test_available():
    config = get_config(namespace='test')

    c = Cassa('test', server_list=config['storage_addresses'])
    c.delete_namespace()

    c = Cassa('test', server_list=config['storage_addresses'])

    key = hashlib.md5('test').hexdigest()
    c.put_available(key)
    ava = c.get_random_available()
    assert ava == key

    c.pop_available(ava)

def test_pop_available():
    config = get_config(namespace='test')

    c = Cassa('test', server_list=config['storage_addresses'])
    c.delete_namespace()

    c = Cassa('test', server_list=config['storage_addresses'])
    
    key = hashlib.md5('test').hexdigest()
    c.pop_available(key)

def test_lengths():
    config = get_config(namespace='test')

    c = Cassa('test', server_list=config['storage_addresses'])
    c.delete_namespace()

    c = Cassa('test', server_list=config['storage_addresses'])

    for i in range(10):
        key = hashlib.md5('test %.10f' % random.random()).hexdigest()
        c.put_task(key, dict(test='hi'))

    assert 10 == c.num_tasks()

def test_more_available_than():
    config = get_config(namespace='test')

    c = Cassa('test', server_list=config['storage_addresses'])
    c.delete_namespace()

    c = Cassa('test', server_list=config['storage_addresses'])

    for i in range(10):
        key = hashlib.md5('test %.10f' % random.random()).hexdigest()
        c.put_available(key)

    assert c.num_available(5) == 5
    assert c.num_available(15) == 10

def test_available_iter():
    config = get_config(namespace='test')

    c = Cassa('test', server_list=config['storage_addresses'])
    c.delete_namespace()

    c = Cassa('test', server_list=config['storage_addresses'])

    for i in range(10):
        key = hashlib.md5('test %.10f' % random.random()).hexdigest()
        c.put_available(key)

    assert len(list(c.available))