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
import json
import uuid
import random
import hashlib
import kazoo.exceptions
from kazoo.client import KazooClient
from kazoo.client import KazooState

## setup loggers
import logging
kazoo_log = logging.getLogger('kazoo')
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
#ch.setFormatter(formatter)
kazoo_log.addHandler(ch)

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
    '''
    Organizes tasks in a globally accessible zookeeper instance

    Each task has a single node under 'tasks' representing its
    existence, which carries the data ID of the current worker

    State is shown by children of /{available,pending}/, which has the
    same name as a node under /tasks/

    When workers register, they get a sequential ephemeral node under
    /workers/ and use that node name as their ID in reserving tasks
    '''

    def __init__(self, config):
        self._config = config
        self._namespace = config['namespace']
        self._zk = KazooClient(config['zookeeper_address'])
        self._zk.start()
        ## save the client_id for reconnect
        self._zk.add_listener(self._restarter)

        self._pending_task_key = None
        ## make a unique ID for this worker that persists across
        ## zookeeper sessions.  Could use a zookeeper generated
        ## sequence number, but using this uuid approach let's us keep
        ## nodes under worker ephemeral without reseting the worker_id
        ## if we lose the zookeeper session.
        self._worker_id = str(uuid.uuid1())
        self.init_all()

    def _restarter(self, state):
        '''
        If connection drops, restart it and keep going
        '''
        if state == KazooState.LOST:
            print 'creating new connection: %r' % state
            self._zk = KazooClient(self._config['zookeeper_address'])
            self._zk.start()

        elif state == KazooState.SUSPENDED:
            print 'state is currently suspended... do something?'

    def _path(self, *path_parts):
        '''
        Returns a path within our namespace.
        namespace/path_parts[0]/parth_parts[1]/...
        '''
        return os.path.join(self._namespace, *path_parts)

    def _register(self):
        '''
        Get an ID for this worker process by creating a sequential
        ephemeral node
        '''
        self._zk.create(
            self._path('workers', self._worker_id), 
            ephemeral=True)

    def __iter__(self):
        '''
        This is the only external interface for getting tasks
        '''
        self._register()
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
                print('won %r' % i_str)
                yield i_str

    def _clear_previous_task(self):
        '''
        If caller iterates past the previous task, then assume it is
        done and remove it.
        '''
        if self._pending_task_key:

            ## update the data
            self.data['state'] = 'completed'
            self.data['owner'] = None

            #print 'entering completion trans'

            ## do a transaction to reset everything at once
            #trans = self._zk.transaction()
            self._zk.delete(self._path('pending', self._pending_task_key))
            self._zk.set(self._path('tasks', self._pending_task_key), json.dumps(self.data))

            #print 'committing completion trans'
            #results = trans.commit()
            #print 'results: %r' % results

            self._pending_task_key = None

    def _random_available_task(self):
        task_keys = self._zk.get_children(self._path('available'))
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
        data, zstat = self._zk.get(self._path('tasks', task_key))

        ## get the payload
        self.data = json.loads(data)

        ## verify payload
        assert self.data['state'] == 'available', self.data['state']
        self.data['state'] = 'pending'
        
        assert self.data['owner'] == None, self.data['owner']
        self.data['owner'] = self._worker_id

        ## keep the value in the pending node for safe keeping
        self._zk.set(self._path('tasks', task_key), json.dumps(self.data))

        ## remove it from the list of available tasks
        self._zk.delete(self._path('available', task_key))
        self._pending_task_key = task_key
        return self.data['i_str']

    def delete_all(self):
        self._zk.delete(self._path(), recursive=True)

    def init_all(self):
        for path in ['tasks', 'available', 'pending', 'mode', 'workers']:
            try:
                self._zk.create(self._path(path), makepath=True)
            except kazoo.exceptions.NodeExistsError:
                pass

    def push(self, *i_strs, **kwargs):
        '''
        Add task to the queue
        '''
        completed = kwargs.get('completed', False)
        count = 0
        for i_str in i_strs:
            ## ignore leading and trailing whitespace
            i_str = i_str.strip()
            if len(i_str) == 0:
                continue
            ## construct a hash to use as the node name
            key = hashlib.md5(i_str).hexdigest()
            ## construct a data payload
            data = dict(
                i_str = i_str,
                state = 'available',
                owner = None,
                )
            if completed:
                data['state'] = 'completed'
            try:
                ## attempt to push it
                try:
                    self._zk.create(self._path('tasks', key), json.dumps(data), makepath=True)
                except kazoo.exceptions.NodeExistsError, exc:
                    if completed:
                        self._zk.set(self._path('tasks', key), json.dumps(data))
                    else:
                        raise(exc)

                ## if succeeded, count it
                count += 1
                if completed:
                    continue
                try:
                    self._zk.create(self._path('available', key), makepath=True)
                except Exception, exc:
                    sys.exit('task(%r) was new put already in available' % key)
            except kazoo.exceptions.NodeExistsError:
                ## if it already exists, then just don't count it
                pass
        return count

    def __len__(self):
        return self._len('tasks')

    def _len(self, state):
        try:
            val, zstat = self._zk.get(self._path(state))
            return zstat.children_count
        except kazoo.exceptions.NoNodeError:
            return 0

    @property
    def _completed(self):
        for child in self._zk.get_children(self._path('tasks')):
            data, zstat = self._zk.get(self._path('tasks', child))
            data = json.loads(data)
            if not isinstance(data, dict):
                print 'whoa... how did we get a string here?'
                data = json.loads(data)
                print data
            if data['state'] == 'completed':
                yield data

    @property
    def counts(self):
        num_completed = 0
        for num_completed, data in enumerate(self._completed):
            pass
        return {
            'tasks': len(self),
            'available': self._len('available'),
            'pending': self._len('pending'),
            'mode': self._read_mode(),
            'completed': num_completed,
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
        if not mode:
            ## default to RUN_FOREVER
            return self.RUN_FOREVER
        else:
            return getattr(self, mode)

    def reset_pending(self):
        '''
        Move anything in 'pending' back to 'available'

        Probably only safe to run this if all workers are off.
        '''
        task_keys = self._zk.get_children(self._path('pending'))
        for task_key in task_keys:
            ## put it back in available
            self._zk.create(self._path('available', task_key))
            self._zk.delete(self._path('pending', task_key))

    def cleanup(self):
        '''
        Go through all the tasks and make sure that the 'available'
        and 'pending' queues match the tasks 'state' property
        '''
        tasks = self._zk.get_children(self._path('tasks'))
        pending = self._zk.get_children(self._path('pending'))
        available = self._zk.get_children(self._path('available'))
        for task_key in tasks:
            task_path = self._path('tasks', task_key)
            data, zstat = self._zk.get(task_path)
            data = json.loads(data)
            assert data['i_str'], repr(data['i_str'])
            if data['state'] == 'completed':
                if task_key in pending:
                    self._zk.delete(self._path('pending', task_key))
                if task_key in available:
                    self._zk.delete(self._path('available', task_key))
            elif data['state'] == 'pending':
                if task_key not in pending:
                    self._zk.create(self._path('pending', task_key))
                if task_key in available:
                    self._zk.delete(self._path('available', task_key))
            elif data['state'] == 'available':
                if task_key in pending:
                    self._zk.delete(self._path('pending', task_key))
                if task_key not in available:
                    self._zk.create(self._path('available', task_key))
            else:
                raise Exception( 'unknown state: %r' % data['state'] )