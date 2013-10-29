'''
Implements a registry-based TaskQueue that is better than the
ZookeeperTaskQueue.  

This software is released under an MIT/X11 open source license.

Copyright 2012-2013 Diffeo, Inc.
'''

import os
import sys
import time
import uuid
import socket
import signal
import random
import kvlayer
import hashlib
import logging
import _memory
import registry
import traceback
import pkg_resources
from streamcorpus import make_stream_time
from _exceptions import TaskQueueUnreachable, GracefulShutdown

logger = logging.getLogger(__name__)

class RegistryTaskQueue(object):
    '''Organizes tasks in a globally accessible registry

    Each task has a single entry in 'tasks' representing its
    existence

    State is shown by entries in 'available' and 'pending', which have the
    same record key as a node in 'tasks'

    When workers register with the registry, it creates a worker_id,
    which this application uses as in reserving tasks
    '''
    def __init__(self, config):
        self._config = config

        self._registry = registry.Registry(config)

        ## create any missing keys
        self.init_all()

        self._pending_task_key = None
        self._continue_running = True
        
        self._sample_from_available = 1

        logger.debug('connecting to cassandra at %r' % config['storage_addresses'])
        self._storage = kvlayer.get_client(config)

    def shutdown(self):
        logger.critical('worker_id=%r RegistryTaskQueue.shutdown has been called')
        self._continue_running = False
        self._return_task()
        self._unregister()

    def _backoff(self, backoff_time):
        time.sleep(backoff_time)

    def __iter__(self):
        '''
        This is the only external interface for getting tasks
        '''
        ## loop until get a task
        task = None

        ## Initial backoff time in secs 
        sleep_time = 1

        while self._continue_running:
            ## check the mode
            mode = self._read_mode()
            if mode is self.TERMINATE:
                break

            task_key, end_count, i_str = self._take_available_task()

            ## mode must be run forever
            if task_key is None:
                assert mode == self.RUN_FOREVER
                sleep_time = min(sleep_time * 2, 128) 
                self._backoff(sleep_time)
                continue

            else:
                ## reset sleep time for next backoff
                sleep_time = 1 

            end_count, i_str = self._win_task(task_key)

            ## if won it, yield
            logger.warn('won %d %r' % (end_count, i_str))
            yield self.data['end_count'], self.data['i_str'], self.data

        assert not self._pending_task_key, \
            'should never break out of worker loop with _pending_task_key=%r' \
            % self._pending_task_key

    @_ensure_connection
    def _return_task(self):
        '''
        "unwin" the current task, so it goes back to 'available' state
        '''
        if self._pending_task_key:
            ## record some data about what is happening -- this should
            ## have a better structure... maybe a thrift?
            self.data['task_returned'] = self._worker_id
            st = make_stream_time()
            self.data['epoch_ticks'] = st.epoch_ticks
            self.data['zulu_timestamp'] = st.zulu_timestamp
            self.data['state'] = 'available'
            self.data['owner'] = None

            ## remove the pending task
            self._registry.delete(self._path('pending', self._pending_task_key))

            ## set the data
            self._storage.put_task(self._pending_task_key, self.data)

            #try:
            #    self._registry.create(self._path('available', self._pending_task_key), makepath=True)
            #except kazoo.exceptions.NodeExistsError:
            #    logger.critical('_return_task encountered NodeExistsError! on %s' % key)
             
            ## put this one back in the potentially large pool of
            ## available task keys in cassandra
            self._put_available(self._pending_task_key)
   
            ## reset our internal state
            self._pending_task_key = None

            logger.critical('worker_id=%r zookeeper session_id=%r _return_task succeeded on %s' \
                                % (self._worker_id, self._registry.client_id, self.data['i_str']))

    @_ensure_connection
    def commit(self, end_count=None, results=None, failure_log=''):
        '''
        If caller iterates past the previous task, then assume it is
        done and remove it.
        '''
        if results is not None:
            assert self._pending_task_key, 'commit(%r) without iterating!?' % results

        if self._pending_task_key:

            ## update the data
            self.data['state'] = 'completed'
            self.data['owner'] = None
            self.data['failure_log'] = failure_log
            self.data['end_count'] = end_count and end_count or 0
            st = make_stream_time()
            self.data['epoch_ticks'] = st.epoch_ticks
            self.data['zulu_timestamp'] = st.zulu_timestamp
            if results:
                self.data['results'] += results

            ## remove the pending task
            self._registry.delete(self._path('pending', self._pending_task_key))

            ## set the data
            self._storage.put_task(self._pending_task_key, self.data)

            try:
                self._registry.create(self._path('completed', self._pending_task_key))
            except kazoo.exceptions.NodeExistsError:
                pass

            ## reset our internal state
            self._pending_task_key = None

    @_ensure_connection
    def partial_commit(self, start_count, end_count, results):

        assert self._pending_task_key, \
            'partial_commit(%d, %d, %r) without pending task' \
            % (start_count, end_count, results)

        assert self.data['end_count'] == start_count, \
            "data['end_count'] = %d != %d = start_count " + \
            'caller failed to start at the right place!?'

        ## update the data
        self.data['end_count'] = end_count
        self.data['results'] += results
        st = make_stream_time()
        self.data['epoch_ticks'] = st.epoch_ticks
        self.data['zulu_timestamp'] = st.zulu_timestamp

        ## set the data
        self._storage.put_task(self._pending_task_key, self.data)

    @_ensure_connection
    def _num_available(self):
        '''
        return estimated number of currently available tasks
        '''
        available = self._registry.get_children(self._path('available'))
        return self._sample_from_available * len(available) ** self._available_levels


    @_ensure_connection
    def _put_available(self, task_key):
        ''' 
        pop a key into the zookeeper available set
        '''
        self._registry.create(self._available_path(task_key), makepath=True)
    
    @_ensure_connection
    def _pop_available(self, task_key):
        ''' 
        pop a key from the zookeeper available set
        '''
        path = self._available_path(task_key)
        self._registry.delete(self._available_path(task_key))
        for x in xrange(self._available_levels):
            path = path.rsplit('/', 1)[0]
            try:
                logger.info( 'trying to delete: %r' % path )
                self._registry.delete(path)
            except kazoo.exceptions.NotEmptyError:
                break

    @_ensure_connection
    def _get_random_available_hier(self):
        ''' 
        get a random task_key from the hierachical zookeeper 
        available set
        '''

        ## Path into available hierarchy
        path = self._path('available')
        available = self._registry.get_children(path)

        ## descend into available hierarchy
        for level in xrange(self._available_levels):

            ## Ensure there are branches this level
            ## before trying to descend 
            if len(available) > 0:
                random_dir = random.sample(available,1)[0]  
                path = os.path.join(path, random_dir)
                logger.info( 'trying path: %r' % path )

                ## Someone could delete this path before
                ## we descent into it.
                try:
                    available = self._registry.get_children(path)
                except kazoo.exceptions.NoNodeError:
                    ## Start over, caller will retry
                    return None
            
            ## We may end up on an empty branch of the hierachy
            else:
                ## Start over, caller will retry
                return None

        len_available = len(available)
        self._sample_from_available = len_available
        if len_available > 0:
            random_task = random.sample(available,1)[0]  
            logger.info( 'found random_task: %r' % random_task )
            return random_task
        else:
            ## Caller will retry
            return None

    @_ensure_connection
    def _take_available_task(self):
        '''
        get an available task
        '''
        with self._registry.transaction() as reg:
            task_id, task_time = reg.popitem_move('available', 'pending', set_time_as_value=True)
        do_shutdown = False
        if mode == self.TERMINATE:
            do_shutdown = True
        elif mode == self.FINISH and task_id is None:
            do_shutdown = True
        if do_shutdown:
            raise GracefulShutdown('mode=%r' % mode)

        assert not self._pending_task_key
        self._pending_task_key = task_key

        self.data = self._storage.get_task(task_key)

        ## verify payload
        assert self.data['state'] == 'available', self.data['state']
        self.data['state'] = 'pending'
        
        assert self.data['owner'] == None, self.data['owner']
        self.data['owner'] = self._worker_id

        ## record data about the run environment, to enable forensics on failed tasks
        self.data['host'] = socket.gethostbyname(socket.gethostname())
        self.data['version'] = pkg_resources.get_distribution("kba.pipeline").version # pylint: disable=E1103
        self.data['config_hash'] = self._config['config_hash']
        self.data['config_json'] = self._config['config_json']
        self.data['VmSize'] = _memory.memory()
        self.data['VmRSS']  = _memory.resident()
        self.data['VmStk']  = _memory.stacksize()

        ## keep the value in the pending node for safe keeping
        self._storage.put_task(task_key, self.data)

        ## could be getting a task that was partially committed
        ## previously, so use previous end_count as new start_count
        do something with task_time

        return task_id, self.data['end_count'], self.data['i_str']

    @_ensure_connection        
    def delete_all(self):
        self._registry.delete(self._path(), recursive=True)
        self._storage.delete_namespace()

    @_ensure_connection        
    def init_all(self):
        for path in ['available', 'completed', 'pending', 'mode', 'workers']:
            if not self._registry.exists(self._path(path)):
                try:
                    self._registry.create(self._path(path), makepath=True)
                except kazoo.exceptions.NodeExistsError, exc:
                    pass

    def purge(self, i_str):
        '''
        Completely expunge a str from the entire queue
        '''
        ## we always strip whitespace off both ends
        i_str = i_str.strip()
        key = self._make_key( i_str )
        try:
            self._storage.pop_task(key)
        except:
            pass
        try:
            self._pop_available(key)
        except:
            pass
        try:
            self._registry.delete(self._path('pending', key))
        except:
            pass
            

    def _make_key(self, i_str):
        '''construct a hash to use as the node name'''
        return hashlib.md5(i_str).hexdigest() # pylint: disable=E1101

    def _make_new_data(self, i_str):
        ## construct a data payload
        data = dict(
            i_str = i_str,
            state = 'available',
            owner = None,
            end_count = 0,
            results = [],
            )
        return data

    def push(self, *i_strs, **kwargs):
        '''
        Add task to the queue

        :param completed: set all these jobs to state-->"completed"

        :param redo: set all these jobs to state-->"available", even
        if completed previously.  Deletes all previous state.

        :param allow_wrong: all i_strs to start with s3:/, which is
        usually wrong, because from_s3_chunks expects paths not URLs
        '''
        completed = kwargs.get('completed', False)
        redo = kwargs.get('redo', False)
        allow_wrong = kwargs.get('allow_wrong_s3', False)
        assert not (completed and redo), 'Programmer Error: cannot set jobs to both "available" and "completed"'
        count = 0
        start_time = time.time()
        for i_str in i_strs:
            ## ignore leading and trailing whitespace
            i_str = i_str.strip()
            if len(i_str) == 0:
                continue

            if i_str.startswith('s3:/') and not (completed or allow_wrong):
                raise Exception('do not load invalid s3 key strings: %r' % i_str)

            ## construct a hash to use as the node name
            key = self._make_key( i_str )

            data = self._make_new_data(i_str)

            if completed:
                data['state'] = 'completed'                

            ## attempt to push it
            created = self._storage.put_task(key, data)
            if created and (completed or redo):
                logger.critical('created %r: %r' % (data['state'], i_str))
                ## must also remove it from available and pending
                self._pop_available(key)
                
                try:
                    self._registry.delete(self._path('pending', key))
                except:
                    pass

                if completed:
                    try:
                        self._registry.create(self._path('completed', key))
                    except kazoo.exceptions.NodeExistsError:
                        pass

                else:
                    try:
                        self._registry.delete(self._path('completed', key))
                    except kazoo.exceptions.NoNodeError:
                        pass

            ## if succeeded, count it
            count += 1
            if count % 100 == 0:
                elapsed = time.time() - start_time
                rate = float(count) / elapsed
                logger.info('%d in %.f sec --> %.1f tasks/sec' % (count, elapsed, rate))

            if completed:
                continue

            try:
                self._put_available(key)
            except Exception, exc:
                sys.exit('task(%r) was new put already in available' % key)

        return count

    def __len__(self):
        return self._storage.num_tasks()

    def _len(self, state):
        try:
            val, zstat = self._registry.get(self._path(state))
            return zstat.children_count
        except kazoo.exceptions.NoNodeError:
            return 0

    def details(self, i_str):
        key = self._make_key(i_str)
        data = self._storage.get_task(key)
        data['task_key'] = key
        return data

    @property
    def pending(self):
        for task_key in self._registry.get_children(self._path('pending')):
            data = self._storage.get_task(task_key)
            yield data

    @property
    def completed(self):
        for data in self._storage.tasks():
            if not isinstance(data, dict):
                logger.critical( 'whoa... how did we get a string here? %r' % data )
            if data['state'] == 'completed' or data['end_count'] > 0 \
                    or len(data['results']) > 0:
                yield data

    @property
    def all_tasks(self):
        return self._storage.tasks()

    def get_tasks_with_prefix(self, key_prefix=''):
        start_time = time.time()
        count = 0
        total_tasks = set()
        for num, task_key in enumerate(self._storage.task_keys):
            if not task_key.startswith(key_prefix):
                continue

            data = self._storage.get_task(task_key)
            if not isinstance(data, dict):
                logger.critical( 'whoa... how did we get a string here? %r' % data )
            yield task_key, data

            count += 1
            if count % 100 == 0:
                elapsed = time.time() - start_time
                if elapsed > 0:
                    rate = float(count) / elapsed
                    undone = len(total_tasks) - count
                    remaining = float(undone) / rate / 3600
                    logger.info('%d in %.1f --> %.1f/sec --> %d (%.1f hrs) remaining'\
                                    % (count, elapsed, rate, undone, remaining))


    @property
    def counts(self):
        #num_completed = 0
        #num_partials = 0
        #num_stream_items_done = 0
        
        #for data in self.completed:
        #    if data['state'] == 'completed':
        #        num_completed += 1
        #    else:
        #        assert data['end_count'] > 0, data['end_count']
        #        num_partials += 1

        #    ## sum the current end_count for both partial and completed
        #    num_stream_items_done += data['end_count'] and data['end_count'] or 0

        return {
            'registered workers': self._len('workers'),
            'tasks': len(self),
            'available': self._num_available(),
            'pending': self._len('pending'),
            'mode': self._read_mode(),
            'completed': self._len('completed'),
            #'partials': num_partials,
            #'stream_items_done': num_stream_items_done,
            }

    @property
    def counts_detailed(self):
        num_completed = 0
        num_partials = 0
        num_stream_items_done = 0
        
        for data in self.completed:
            if data['state'] == 'completed':
                num_completed += 1
            else:
                assert data['end_count'] > 0, data['end_count']
                num_partials += 1

            ## sum the current end_count for both partial and completed
            num_stream_items_done += data['end_count'] and data['end_count'] or 0

        return {
            'registered workers': self._len('workers'),
            'tasks': len(self),
            'available': self._num_available(),
            'pending': self._len('pending'),
            'mode': self._read_mode(),
            'completed': num_completed,
            'partials': num_partials,
            'stream_items_done': num_stream_items_done,
            }        

    def set_mode(self, mode):
        '''
        Signal to all workers how to behave
        '''
        assert hasattr(self, mode), mode
        self._registry.set_mode(mode)

    RUN_FOREVER = 'RUN_FOREVER'
    FINISH = 'FINISH'
    TERMINATE = 'TERMINATE'

    @_ensure_connection
    def _read_mode(self):
        '''
        Get mode from registry
        '''
        return getattr(self, self._registry.get_mode())

    def reset_all_to_available(self, key_prefix=''):
        '''
        Move every task that startswith(key_prefix) to 'available'

        Probably only safe to run this if all workers are off.
        '''
        pending = self._registry.get_children(self._path('pending'))
        completed = self._registry.get_children(self._path('completed'))

        approx_num_assigned_tasks = int(float(self._storage.num_tasks()) / \
                                            max(1, 16 * len(key_prefix)))
        count = 0
        start_time = time.time()
        for data in self._storage.tasks(key_prefix):
            count += 1
            if count % 100 == 0:
                elapsed = time.time() - start_time
                if elapsed > 0:
                    rate = float(count) / elapsed
                    undone = approx_num_assigned_tasks - count
                    remaining = float(undone) / rate / 3600
                    logger.info('cleaning up %d in %.1f --> %.1f/sec --> %d (%.3f hrs) remaining'\
                                    % (count, elapsed, rate, undone, remaining))

            task_key = data['task_key']

            ## if the state is not already available, or if it has
            ## anything resembling partial work:
            if data['state'] != 'available' or data['end_count'] > 0 \
                    or len(data['results']) > 0 or data.get('failure_log', ''):
                ## recreate new record:
                data = self._make_new_data(data['i_str'])
                self._storage.put_task(task_key, data)

            task_key = data['task_key']

            ## make sure it is in available
            self._put_available(task_key)

            ## make sure it is not in pending or completed
            if task_key in pending:
                try:
                    self._registry.delete(self._path('pending', task_key))
                except kazoo.exceptions.NoNodeError:
                    pass
            if task_key in completed:
                try:
                    self._registry.delete(self._path('completed', task_key))
                except kazoo.exceptions.NoNodeError:
                    pass

    def reset_pending(self, key_prefix=''):
        '''
        Move every task that startswith(key_prefix) in 'pending' back to 'available'

        Probably only safe to run this if all workers are off.
        '''
        ## should assert that no workers are working
        #workers = self._registry.get_children(self._path('workers'))
        #workers = set(workers)
        #print workers
        #workers.remove( self._worker_id )
        #assert not workers, 'cannot reset while workers=%r' % workers

        start_time = time.time()
        count = 0
        total_pending = set()
        pending = self._registry.get_children(self._path('pending'))
        for task_key in pending:
            if task_key.startswith(key_prefix):
                total_pending.add(task_key)

        for task_key in total_pending:
            ## put it back in available
            data = self._storage.get_task(task_key)
            data['state'] = 'available'
            data['owner'] = None
            self._storage.put_task(task_key, data)
            try:
                self._put_available(task_key)
            except kazoo.exceptions.NodeExistsError:
                pass
            try:
                self._registry.delete(self._path('pending', task_key))
            except kazoo.exceptions.NoNodeError:
                pass
            count += 1
            if count % 100 == 0:
                elapsed = time.time() - start_time
                if elapsed > 0:
                    rate = float(count) / elapsed
                    undone = len(total_pending) - count
                    remaining = float(undone) / rate / 3600
                    logger.critical('reset pending-->available %d in %.1f --> %.1f/sec --> %d (%.1f hrs) remaining'\
                                    % (count, elapsed, rate, undone, remaining))

    def cleanup(self, key_prefix=''):
        '''
        Go through all the tasks and make sure that the 'available'
        and 'pending' queues match the tasks 'state' property
        '''
        pending = self._registry.get_children(self._path('pending'))
        completed = self._registry.get_children(self._path('completed'))
        start_time = time.time()
        count = 0

        approx_num_assigned_tasks = int(float(self._storage.num_tasks()) / (16 * len(key_prefix)))

        for task_key in self._storage.task_keys:
            if not task_key.startswith(key_prefix):
                #logger.debug('skipping %s' % task_key)
                continue

            logger.debug('evaluatinging %s' % task_key)
            data = self._storage.get_task(task_key)

            logger.debug('got data %r' % data)

            count += 1
            if count % 100 == 0:
                elapsed = time.time() - start_time
                if elapsed > 0:
                    rate = float(count) / elapsed
                    undone = approx_num_assigned_tasks - count
                    remaining = float(undone) / rate / 3600
                    logger.info('cleaning up %d in %.1f --> %.1f/sec --> %d (%.3f hrs) remaining'\
                                    % (count, elapsed, rate, undone, remaining))
            assert data['i_str'], repr(data['i_str'])

            if data['state'] == 'completed':
                if task_key not in completed:
                    self._registry.create(self._path('completed', task_key))
                if task_key in pending:
                    self._registry.delete(self._path('pending', task_key))
                if self._storage.in_available(task_key):
                    self._pop_available(task_key)

            elif data['state'] == 'pending':                    
                if task_key in completed:
                    self._registry.delete(self._path('completed', task_key))
                if task_key not in pending:
                    self._registry.create(self._path('pending', task_key))
                if self._storage.in_available(task_key):
                    self._pop_available(task_key)

            elif data['state'] == 'available':
                logger.debug('handling state == %r' % data['state'])
                if task_key in completed:
                    self._registry.delete(self._path('completed', task_key))
                if task_key in pending:
                    self._registry.delete(self._path('pending', task_key))
                if self._storage.in_available(task_key):
                    logger.debug('attempting to _put_available %r' % task_key)
                    self._put_available(task_key)

            else:
                raise Exception( 'unknown state: %r' % data['state'] )

    def clear_registered_workers(self):
        '''
        Delete every registered worker node
        '''
        for worker_id in self._registry.get_children(self._path('workers')):
            try:
                self._registry.delete(self._path('workers', worker_id))
            except kazoo.exceptions.NoNodeError:
                pass
