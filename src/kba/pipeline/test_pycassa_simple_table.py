
import hashlib
from _pycassa_simple_table import Cassa

def test_tasks():
    c = Cassa('test')
    c.delete_namespace()

    c = Cassa('test')

    key = hashlib.md5('test').hexdigest()
    c.put_task(key, dict(test='hi'))
    for t in c.tasks:
        print t

def test_available():
    c = Cassa('test')
    c.delete_namespace()

    c = Cassa('test')

    key = hashlib.md5('test').hexdigest()
    c.put_available(key)
    ava = c.get_random_available()
    assert ava == key

