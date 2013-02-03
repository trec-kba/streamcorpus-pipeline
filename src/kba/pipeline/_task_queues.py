'''
Provide the first stage in any pipeline: a source of strings that an
extractor can use to find a Chunk file, typically a path in a
filesystem.

This software is released under an MIT/X11 open source license.

Copyright 2012 Diffeo, Inc.
'''

import os
import sys
import time
import random
import hashlib
import kazoo.exceptions
from kazoo.client import KazooClient


def stdin(config):
    '''
    A task_queue-type stage that wraps sys.stdin
    '''
    for i_str in sys.stdin:
        ## remove trailing newlines
        if i_str.endswith('\n'):
            i_str = i_str[:-1]
        yield i_str

class ZookeeperTaskQueue(object):
    def __init__(self, config):
        self._config = config
        self._namespace = config['namespace']
        self._zk = KazooClient(config['zookeeper_address'])
        self._zk.start()
        self._pending_task_key = None
        if 'delete_all' in config and config['delete_all']:
            self.delete_all()
            self.init_all()

    def _path(self, category, node_name=None):
        '''
        Returns a path within our namespace.
        namespace/category

        or if node_name is not None:
        namespace/category/node_name
        '''
        if node_name is not None:
            return os.path.join(self._namespace, category, node_name)
        else:
            return os.path.join(self._namespace, category)

    def __iter__(self):
        ## loop until get a task
        task = None
        while True:
            ## clear the last task
            self._clear_previous_task()

            ## get a task
            task_key = self._random_available_task()

            ## check the mode
            mode = self._read_mode()
            if mode is self.TERMINATE:
                break
            elif mode is self.FINISH:
                if task_key is None:
                    break

            else:
                ## only here to we wait
                if task_key is None:
                    time.sleep(2)

            ## attempt to win the task
            i_str = self._win_task(task_key)

            ## if won it, yield
            if i_str is not None:
                yield i_str

    def _clear_previous_task(self):
        '''
        If caller iterates past the previous task, then assume it is
        done and remove it.
        '''
        if self._pending_task_key:
            self._zk.create(self._path('completed', self._pending_task_key), makepath=True)
            self._zk.delete(self._path('pending', self._pending_task_key))
            self._pending_task_key = None

    def _random_available_task(self):
        task_keys = self._zk.get_children(self._path('available_tasks'))
        if not task_keys:
            return None
        else:
            return random.choice( task_keys )

    def _win_task(self, task_key):
        try:
            self._zk.create(self._path('pending', task_key), makepath=True)
        except kazoo.exceptions.NodeExistsError:
            return None
        ## won it!
        i_str, zstat = self._zk.get(self._path('available_tasks', task_key))

        ## keep the value in the pending node for safe keeping
        self._zk.set(self._path('pending', task_key), i_str)

        ## remove it from the list of available tasks
        self._zk.delete(self._path('available_tasks', task_key))
        self._pending_task_key = task_key
        return i_str

    def delete_all(self):
        self._zk.delete(self._path('available_tasks'), recursive=True)
        self._zk.delete(self._path('pending'), recursive=True)
        self._zk.delete(self._path('completed'), recursive=True)
        self._zk.delete(self._path('mode'), recursive=True)

    def init_all(self):
        self._zk.create(self._path('available_tasks'), makepath=True)
        self._zk.create(self._path('pending'), makepath=True)
        self._zk.create(self._path('completed'), makepath=True)
        self._zk.create(self._path('mode'), self.RUN_FOREVER, makepath=True)

    def push(self, i_str):
        '''
        Add tasks to the queue
        '''
        hash = hashlib.md5(i_str).hexdigest()
        try:
            self._zk.create(self._path('available_tasks', hash), i_str, makepath=True)
            return 1
        except kazoo.exceptions.NodeExistsError:
            return 0

    def __len__(self):
        return self._len('available_tasks')

    def _len(self, state):
        try:
            val, zstat = self._zk.get(self._path(state))
            return zstat.children_count
        except kazoo.exceptions.NoNodeError:
            return 0

    @property
    def counts(self):
        return {
            'available_tasks': len(self),
            'pending': self._len('pending'),
            'completed': self._len('completed'),
            }

    def set_mode(self, mode):
        '''
        Signal to all workers how to behave
        '''
        assert hasattr(self, mode), mode
        self._zk.set(self._path('mode'), mode)

    RUN_FOREVER = 'RUN_FOREVER'
    FINISH = 'FINISH'
    TERMINATE = 'TERMINATE'

    def _read_mode(self):
        '''
        Get the mode from ZK and convert back to a class property
        '''
        mode, zstat = self._zk.get(self._path('mode'))
        return getattr(self, mode)
