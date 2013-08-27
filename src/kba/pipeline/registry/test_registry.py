
import os
import sys
import yaml
import pytest
from .._logging import logger

from ..make_namespace_string import make_namespace_string
namespace = make_namespace_string()

from . import Registry

@pytest.fixture(scope='function')
def registry(request):
    config_path = os.path.join(os.path.dirname(__file__), 'config_registry.yaml')
    if not os.path.exists(config_path):
        sys.exit('failed to find %r' % config_path)

    try:
        config = yaml.load(open(config_path))
    except Exception, exc:
        sys.exit('failed to load %r: %s' % (config_path, exc))

    config['namespace'] = namespace

    def fin():
        registry = Registry(config)
        registry.delete_namespace()
        logger.info('tearing down %r' % config['namespace'])
    request.addfinalizer(fin)


    registry = Registry(config)
    return registry

def test_registry_update_pull(registry):
    test_dict = dict(cars=10, houses=5)

    with registry.lock() as session:
        session.update('test_dict', test_dict)
        assert session.pull('test_dict') == test_dict

    with registry.lock() as session:
        assert session.pull('test_dict') == test_dict


def test_registry_popitem(registry):
    test_dict = dict(cars=10, houses=5)

    with registry.lock() as session:
        session.update('test_dict', test_dict)
        assert session.pull('test_dict') == test_dict

    recovered = set()
    with registry.lock() as session:
        recovered.add(session.popitem('test_dict'))
        recovered.add(session.popitem('test_dict'))

        assert recovered == set(test_dict.items())


def test_registry_popitem_move(registry):
    test_dict = dict(cars=10, houses=5)

    with registry.lock() as session:
        session.update('test_dict', test_dict)
        assert session.pull('test_dict') == test_dict

    recovered = set()
    with registry.lock() as session:
        recovered.add(session.popitem_move('test_dict', 'second'))
        assert session.len('test_dict') == 1
        assert session.len('second') == 1
        recovered.add(session.popitem_move('test_dict', 'second'))
        assert session.len('test_dict') == 0
        assert session.len('second') == 2

        assert recovered == set(test_dict.items())
        assert recovered == set(session.pull('second').items())


def test_registry_move(registry):
    test_dict = dict(cars=10, houses=5)

    with registry.lock() as session:
        session.update('test_dict', test_dict)
        assert session.pull('test_dict') == test_dict

    recovered = set()
    with registry.lock() as session:
        session.move('test_dict', 'second', dict(cars=3))
        assert session.len('test_dict') == 1
        assert session.len('second') == 1
        session.move('test_dict', 'second', dict(houses=2))
        assert session.len('test_dict') == 0
        assert session.len('second') == 2

        assert dict(cars=3, houses=2) == session.pull('second')


    
