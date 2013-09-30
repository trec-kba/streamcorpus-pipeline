'''
Provide the first stage in any pipeline: a source of strings that an
extractor can use to find a Chunk file, typically a path in a
filesystem.

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
import hashlib
import logging
import _memory
import traceback
import pkg_resources
import kazoo.exceptions
from kazoo.client import KazooClient
from kazoo.client import KazooState
from streamcorpus import make_stream_time
from _exceptions import TaskQueueUnreachable, GracefulShutdown

from _pycassa_simple_table import Cassa

logger = logging.getLogger(__name__)

class TaskQueue(object):
    '''
    task_queue base class
    '''
    def __init__(self, config):
        self.config = config

    def __iter__(self):
        raise NotImplementedError('subclasses of TaskQueue must provide __iter__')

    def commit(self, end_count=None, results=None, failed=False):
        '''
        record results of processing i_str
        :param end_count: number of StreamItems generated from processing i_str
        :type end_count: int
        :type results: list( of strings )
        '''
        #could print (i_str --> results)
        pass

    def partial_commit(self, start_count=None, end_count=None, results=None):
        '''
        record a result of processing part of i_str

        :param start, end: the start and end step numbers taken in
        generating the partial result for i_str

        :param end_count: number of StreamItems generated from
        processing i_str up to most recent previous partial_commit

        :type start_count: int

        :param end_count: number of StreamItems generated from
        processing i_str so far

        :type end_count: int
        :type results: string
        '''
        #could print (i_str --> results)
        pass

    def shutdown(self):
        '''
        cleanly exit, return any open tasks to non-pending if necessary
        '''
        pass

class stdin(TaskQueue):
    '''
    A task_queue-type stage that wraps sys.stdin
    '''
    def __iter__(self):
        data = {}
        for i_str in sys.stdin:
            i_str = i_str.strip()
            ## yield the start position of zero, because we have not
            ## persistence mechanism in this queue for partial_commit
            yield 0, i_str, data

class itertq(TaskQueue):
    '''
    A task_queue-type stage that wraps an iterable
    '''
    def __iter__(self):
        data = {}
        for i_str in self.config['inputs_path']:
            ## yield the start position of zero, because we have not
            ## persistence mechanism in this queue for partial_commit
            yield 0, i_str, data

def _ensure_connection(func):
    '''
    Decorator for methods on ZookeeperTaskQueue that attemps to
    restart connection if kazoo.exceptions.ConnectionLoss happens.
    '''
    def wrapped_func(self, *args, **kwargs):
        delay = 0.1
        max_tries = 30
        tries = 0
        while tries < max_tries:
            try:
                return func(self, *args, **kwargs)
            except kazoo.exceptions.ConnectionLoss, exc:
                logger.critical('worker_id=%r zookeeper session_id=%r %r --> %s\n ATTEMPT reconnect...'\
                                    % (self._worker_id, self._zk.client_id, func, traceback.format_exc(exc)))
                try:
                    self._restarter(self._zk.state)
                    logger.critical('worker_id=%r zookeeper session_id=%r COMPLETED reconnect'\
                                        % (self._worker_id, self._zk.client_id))
                    continue 
                except Exception, exc:
                    logger.critical('worker_id=%r zookeeper session_id=%r FAILED reconnect --> %s'\
                                        % (self._worker_id, self._zk.client_id, traceback.format_exc(exc)))
                tries += 1
                delay *= 2
                time.sleep(delay)

        ## if we get here, then we hit max_tries:
        raise TaskQueueUnreachable()

    return wrapped_func


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

    @classmethod
    def valid_config(cls, config, raise_on_error=False):
        config = config.get('zookeeper')
        if not config:
            if raise_on_error:
                raise Exception('missing "zookeeper" section in config')
            return False
        if config.get('namespace') is None:
            if raise_on_error:
                raise Exception('"zookeeper" section missing "namspace" value')
            return False
        if 'zookeeper_addresses' in config:
            return True
        elif 'zookeeper_address' in config:
            return True
        else:
            if raise_on_error:
                raise Exception('"zookeeper" section needs zookeeper_address or zookeeper_addresses value')
            return False

    def __init__(self, config):
        self.valid_config(config, raise_on_error=True)
        config = config['zookeeper']
        self._config = config
        self._namespace = config['namespace']
        if 'zookeeper_addresses' in config:
            self.addresses = ','.join(config['zookeeper_addresses'])
        elif 'zookeeper_address' in config:
            self.addresses = config['zookeeper_address']
        else:
            raise Exception('must specify zookeeper_address(es) in config: %r' % config)

        logger.debug('connecting to zookeeper at %r' % self.addresses)
        self._zk = KazooClient(self.addresses,
                               timeout = config['zookeeper_timeout'],
                               )
        self._zk.start(timeout = config['zookeeper_timeout'])
        ## save the client_id for reconnect
        self._zk.add_listener(self._restarter)

        ## create any missing keys
        self.init_all()

        self._pending_task_key = None
        self._continue_running = True
        
        ## number of levels in the available hierarchy
        self._available_levels = 2
       
        self._sample_from_available = 1

        ## make a unique ID for this worker that persists across
        ## zookeeper sessions.  Could use a zookeeper generated
        ## sequence number, but using this uuid approach let's us keep
        ## nodes under worker ephemeral without reseting the worker_id
        ## if we lose the zookeeper session.
        self._worker_id = str(uuid.uuid1())

        logger.debug('worker_id=%r zookeeper session_id=%r starting up on hostname=%r' % (self._worker_id, self._zk.client_id, socket.gethostbyname(socket.gethostname())))

        logger.debug('connecting to cassandra at %r' % config['storage_addresses'])
        self._cassa = Cassa(config['namespace'], config['storage_addresses'])

    def shutdown(self):
        logger.critical('worker_id=%r zookeeper session_id=%r ZookeeperTaskQueue.shutdown has been called')
        self._continue_running = False
        self._return_task()
        self._unregister()
        self._zk.stop()
        logger.critical('worker_id=%r zookeeper session_id=%r closed zookeeper client' % (self._worker_id, self._zk.client_id))

    def _restarter(self, state):
        '''
        If connection drops, restart it and keep going
        '''
        if state == KazooState.LOST:
            logger.warn( 'creating new connection: %r' % state )
            self._zk = KazooClient(self.addresses,
                                   timeout=self._config['zookeeper_timeout'])
            self._zk.start(timeout=self._config['zookeeper_timeout'])

        elif state == KazooState.SUSPENDED:
            logger.warn( 'state is currently suspended... attempting start(%d)' % (
                    self._config['zookeeper_timeout']))

            client_id = self._zk.client_id
            self._zk = KazooClient(self.addresses,
                                   timeout=self._config['zookeeper_timeout'],
                                   client_id = client_id,
                                   )
            self._zk.start(timeout=self._config['zookeeper_timeout'])

    def _path(self, *path_parts):
        '''
        Returns a path within our namespace.
        namespace/path_parts[0]/parth_parts[1]/...
        '''
        return os.path.join(self._namespace, *path_parts)

    def _available_path(self, task_key):
        '''
        Return path into available hierarchy for a given key.
        '''
        split_task_key = []
        split_len = 2
        for level in xrange(self._available_levels):
            split_task_key.append(task_key[level * split_len: (level+1) * split_len])
        else:
            split_task_key.append(task_key)
        return self._path('available', *split_task_key)

    @_ensure_connection        
    def _register(self):
        '''
        Get an ID for this worker process by creating a sequential
        ephemeral node
        '''
        self._zk.create(
            self._path('workers', self._worker_id), 
            ephemeral=True,
            makepath=True)

    @_ensure_connection        
    def _unregister(self):
        '''
        Get an ID for this worker process by creating a sequential
        ephemeral node
        '''
        try:
            self._zk.delete(self._path('workers', self._worker_id))
        except kazoo.exceptions.NoNodeError:
            logger.critical('attempted to _unregsiter node already gone: %r'\
                                % self._path('workers', self._worker_id))
            pass

    def _backoff(self, backoff_time):
        time.sleep(backoff_time)

    def __iter__(self):
        '''
        This is the only external interface for getting tasks
        '''
        self._register()
        ## loop until get a task
        task = None

        ## Initial backoff time in secs 
        sleep_time = 1

        while self._continue_running:
            ## check the mode
            mode = self._read_mode()
            if mode is self.TERMINATE:
                break

            ## get a task
            task_key = self._random_available_task()

            logger.critical('considering task_key=%r' % task_key)

            ## happens once for each worker at the end
            if mode is self.FINISH and task_key is None:
                break

            ## mode must be run forever
            if task_key is None:
                assert mode == self.RUN_FOREVER
                time.sleep(2)
                continue

            logger.critical('attempting to win task')
            ## attempt to win the task
            end_count, i_str = self._win_task(task_key)

            ## if won it, yield
            if i_str is not None:
                logger.warn('won %d %r' % (end_count, i_str))
                yield end_count, i_str, self.data

                ## reset sleep time for next backoff loop
                sleep_time = 1 

            else:
                ## backoff before trying again
                sleep_time = min(sleep_time * 2, 128) 
                self._backoff(sleep_time)

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
            self._zk.delete(self._path('pending', self._pending_task_key))

            ## set the data
            self._cassa.put_task(self._pending_task_key, self.data)

            #try:
            #    self._zk.create(self._path('available', self._pending_task_key), makepath=True)
            #except kazoo.exceptions.NodeExistsError:
            #    logger.critical('_return_task encountered NodeExistsError! on %s' % key)
             
            ## put this one back in the potentially large pool of
            ## available task keys in cassandra
            self._put_available(self._pending_task_key)
   
            ## reset our internal state
            self._pending_task_key = None

            logger.critical('worker_id=%r zookeeper session_id=%r _return_task succeeded on %s' \
                                % (self._worker_id, self._zk.client_id, self.data['i_str']))

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
            self._zk.delete(self._path('pending', self._pending_task_key))

            ## set the data
            self._cassa.put_task(self._pending_task_key, self.data)

            try:
                self._zk.create(self._path('completed', self._pending_task_key))
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
        self._cassa.put_task(self._pending_task_key, self.data)

    @_ensure_connection
    def _num_available(self):
        '''
        return estimated number of currently available tasks
        '''
        available = self._zk.get_children(self._path('available'))
        return self._sample_from_available * len(available) ** self._available_levels


    @_ensure_connection
    def _put_available(self, task_key):
        ''' 
        pop a key into the zookeeper available set
        '''
        self._zk.create(self._available_path(task_key), makepath=True)
    
    @_ensure_connection
    def _pop_available(self, task_key):
        ''' 
        pop a key from the zookeeper available set
        '''
        path = self._available_path(task_key)
        self._zk.delete(self._available_path(task_key))
        for x in xrange(self._available_levels):
            path = path.rsplit('/', 1)[0]
            try:
                logger.info( 'trying to delete: %r' % path )
                self._zk.delete(path)
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
        available = self._zk.get_children(path)

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
                    available = self._zk.get_children(path)
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
    def _random_available_task(self):
        '''
        get an available task -- selected at random
        '''
        logger.debug('attempting to find random available task')
        num_workers = len(self._zk.get_children(self._path('workers')))
        logger.debug('num_workers is %d' % num_workers)
        num_available = self._num_available()
        logger.debug('num_available is %d' % num_available)
        if num_available < num_workers:
            ## fewer tasks than workers
            ## consider shutting down
            mode = self._read_mode()
            logger.debug('mode is %r' % mode)
            do_shutdown = False
            if mode == self.TERMINATE:
                do_shutdown = True
            elif mode == self.FINISH and \
                    random.random() < self._config.get('finish_ramp_down_fraction', 0.1):

                if num_workers > self._config.get('min_workers', 0):
                    logger.critical('num_workers=%d > %d=min_workers: %r' % (
                            num_workers, self._config.get('min_workers', 0), self._config))
                    do_shutdown = True

            elif mode == self.RUN_FOREVER:
                ## maybe backoff here?
                pass
            if do_shutdown:
                raise GracefulShutdown('mode=%r num_workers=%d > %d=num_available' \
                                           % (mode, num_workers, num_available))

        ## if no tasks are available, the __iter__ loop will either
        ## exit or sleep
        if num_available == 0:
            return None
        else:
            return self._get_random_available_hier()

    @_ensure_connection        
    def _win_task(self, task_key):
        logger.debug('attempting to win %r' % task_key)
        assert not self._pending_task_key
        try:
            self._zk.create(self._path('pending', task_key), makepath=True)
        except kazoo.exceptions.NodeExistsError:
            return None, None
        ## won it!
        self.data = self._cassa.get_task(task_key)

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
        self._cassa.put_task(task_key, self.data)

        ## remove it from the list of available tasks
        self._pop_available(task_key)

        self._pending_task_key = task_key

        ## could be getting a task that was partially committed
        ## previously, so use previous end_count as new start_count
        return self.data['end_count'], self.data['i_str']

    @_ensure_connection        
    def delete_all(self):
        self._zk.delete(self._path(), recursive=True)
        self._cassa.delete_namespace()

    @_ensure_connection        
    def init_all(self):
        for path in ['available', 'completed', 'pending', 'mode', 'workers']:
            if not self._zk.exists(self._path(path)):
                try:
                    self._zk.create(self._path(path), makepath=True)
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
            self._cassa.pop_task(key)
        except:
            pass
        try:
            self._pop_available(key)
        except:
            pass
        try:
            self._zk.delete(self._path('pending', key))
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
            created = self._cassa.put_task(key, data)
            if created and (completed or redo):
                logger.critical('created %r: %r' % (data['state'], i_str))
                ## must also remove it from available and pending
                self._pop_available(key)
                
                try:
                    self._zk.delete(self._path('pending', key))
                except:
                    pass

                if completed:
                    try:
                        self._zk.create(self._path('completed', key))
                    except kazoo.exceptions.NodeExistsError:
                        pass

                else:
                    try:
                        self._zk.delete(self._path('completed', key))
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
        return self._cassa.num_tasks()

    def _len(self, state):
        try:
            val, zstat = self._zk.get(self._path(state))
            return zstat.children_count
        except kazoo.exceptions.NoNodeError:
            return 0

    def details(self, i_str):
        key = self._make_key(i_str)
        data = self._cassa.get_task(key)
        data['task_key'] = key
        return data

    @property
    def pending(self):
        for task_key in self._zk.get_children(self._path('pending')):
            data = self._cassa.get_task(task_key)
            yield data

    @property
    def completed(self):
        for data in self._cassa.tasks():
            if not isinstance(data, dict):
                logger.critical( 'whoa... how did we get a string here? %r' % data )
            if data['state'] == 'completed' or data['end_count'] > 0 \
                    or len(data['results']) > 0:
                yield data

    @property
    def all_tasks(self):
        return self._cassa.tasks()

    def get_tasks_with_prefix(self, key_prefix=''):
        start_time = time.time()
        count = 0
        total_tasks = set()
        for num, task_key in enumerate(self._cassa.task_keys):
            if not task_key.startswith(key_prefix):
                continue

            data = self._cassa.get_task(task_key)
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
        self._zk.set(self._path('mode'), mode)

    RUN_FOREVER = 'RUN_FOREVER'
    FINISH = 'FINISH'
    TERMINATE = 'TERMINATE'

    @_ensure_connection
    def _read_mode(self):
        '''
        Get the mode from ZK and convert back to a class property
        '''
        mode, zstat = self._zk.get(self._path('mode'))
        if not mode:
            ## default to RUN_FOREVER
            return self.RUN_FOREVER
        else:
            ## convert mode string into class property
            return getattr(self, mode)

    def reset_all_to_available(self, key_prefix=''):
        '''
        Move every task that startswith(key_prefix) to 'available'

        Probably only safe to run this if all workers are off.
        '''
        pending = self._zk.get_children(self._path('pending'))
        completed = self._zk.get_children(self._path('completed'))

        approx_num_assigned_tasks = int(float(self._cassa.num_tasks()) / \
                                            max(1, 16 * len(key_prefix)))
        count = 0
        start_time = time.time()
        for data in self._cassa.tasks(key_prefix):
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
                self._cassa.put_task(task_key, data)

            task_key = data['task_key']

            ## make sure it is in available
            self._put_available(task_key)

            ## make sure it is not in pending or completed
            if task_key in pending:
                try:
                    self._zk.delete(self._path('pending', task_key))
                except kazoo.exceptions.NoNodeError:
                    pass
            if task_key in completed:
                try:
                    self._zk.delete(self._path('completed', task_key))
                except kazoo.exceptions.NoNodeError:
                    pass

    def reset_pending(self, key_prefix=''):
        '''
        Move every task that startswith(key_prefix) in 'pending' back to 'available'

        Probably only safe to run this if all workers are off.
        '''
        ## should assert that no workers are working
        #workers = self._zk.get_children(self._path('workers'))
        #workers = set(workers)
        #print workers
        #workers.remove( self._worker_id )
        #assert not workers, 'cannot reset while workers=%r' % workers

        start_time = time.time()
        count = 0
        total_pending = set()
        pending = self._zk.get_children(self._path('pending'))
        for task_key in pending:
            if task_key.startswith(key_prefix):
                total_pending.add(task_key)

        for task_key in total_pending:
            ## put it back in available
            data = self._cassa.get_task(task_key)
            data['state'] = 'available'
            data['owner'] = None
            self._cassa.put_task(task_key, data)
            try:
                self._put_available(task_key)
            except kazoo.exceptions.NodeExistsError:
                pass
            try:
                self._zk.delete(self._path('pending', task_key))
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
        pending = self._zk.get_children(self._path('pending'))
        completed = self._zk.get_children(self._path('completed'))
        start_time = time.time()
        count = 0

        approx_num_assigned_tasks = int(float(self._cassa.num_tasks()) / (16 * len(key_prefix)))

        for task_key in self._cassa.task_keys:
            if not task_key.startswith(key_prefix):
                #logger.debug('skipping %s' % task_key)
                continue

            logger.debug('evaluatinging %s' % task_key)
            data = self._cassa.get_task(task_key)

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
                    self._zk.create(self._path('completed', task_key))
                if task_key in pending:
                    self._zk.delete(self._path('pending', task_key))
                if self._cassa.in_available(task_key):
                    self._pop_available(task_key)

            elif data['state'] == 'pending':                    
                if task_key in completed:
                    self._zk.delete(self._path('completed', task_key))
                if task_key not in pending:
                    self._zk.create(self._path('pending', task_key))
                if self._cassa.in_available(task_key):
                    self._pop_available(task_key)

            elif data['state'] == 'available':
                logger.debug('handling state == %r' % data['state'])
                if task_key in completed:
                    self._zk.delete(self._path('completed', task_key))
                if task_key in pending:
                    self._zk.delete(self._path('pending', task_key))
                if self._cassa.in_available(task_key):
                    logger.debug('attempting to _put_available %r' % task_key)
                    self._put_available(task_key)

            else:
                raise Exception( 'unknown state: %r' % data['state'] )

    def clear_registered_workers(self):
        '''
        Delete every registered worker node
        '''
        for worker_id in self._zk.get_children(self._path('workers')):
            try:
                self._zk.delete(self._path('workers', worker_id))
            except kazoo.exceptions.NoNodeError:
                pass
