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
import logging
import _memory
import rejester
import traceback
from streamcorpus import make_stream_time
from _exceptions import TaskQueueUnreachable, GracefulShutdown

logger = logging.getLogger('streamcorpus.pipeline.RejesterTaskQueue')

class RejesterTaskQueue(object):
    '''Organizes tasks in a globally accessible registry

    Each task has a single entry in 'tasks' representing its
    existence
    '''
    work_spec_name = 'foo'
    def __init__(self, config):
        self._config = config

        self._task_master = rejester.TaskMaster(config)

        self._work_unit = None
        self._continue_running = True

    def shutdown(self):
        logger.critical('worker_id=%r RegistryTaskQueue.shutdown has been called')
        self._continue_running = False
        self._work_unit.update(lease_time=0)
        self._task_master.unregister_worker()

    def _backoff(self, backoff_time):
        time.sleep(backoff_time)

    def __iter__(self):
        '''This iterator interface is the only external interface to
        TaskQueue.  Once instantiated, a worker simply loops over
        WorkItem objects coming out of the 
        '''
        ## loop until get a task
        task = None

        ## Initial backoff time in secs 
        sleep_time = 1

        while self._continue_running:
            ## check the mode
            mode = self._read_mode()
            if self._task_master.mode_is_terminate()
                break

            self._work_unit = self._task_master.get_work(available_gb=available_gb)

            if self._work_unit is None:
                sleep_time = min(sleep_time * 2, 128) 
                self._backoff(sleep_time)
                continue

            ## reset sleep time for next backoff
            sleep_time = 1 

            logger.warn('won %r', self._work_unit)
            data = self._work_unit.data
            yield data['end_count'], data['i_str'], data

        if self._pending_task_key:
            raise ProgrammerError(
                'should never break out of worker loop with _pending_task_key=%r'
                % self._pending_task_key)


    def commit(self, end_count=None, results=None, failure_log=''):
        '''
        If caller iterates past the previous task, then assume it is
        done and remove it.
        '''
        if results is not None and not self._pending_task_key:
            raise ProgrammerError('commit(%r) without iterating!?' % results)

        if self._pending_task_key:

            ## update the data
            data = self._work_unit
            data['state'] = 'completed'
            data['owner'] = None
            data['failure_log'] = failure_log
            data['end_count'] = end_count and end_count or 0
            st = make_stream_time()
            data['epoch_ticks'] = st.epoch_ticks
            data['zulu_timestamp'] = st.zulu_timestamp
            if results:
                data['results'] += results

            ## remove the pending task
            self._work_unit.finish()

            ## reset our internal state
            self._work_unit = None


    def partial_commit(self, start_count, end_count, results):

        if not self._work_unit:
            raise ProgrammerError(
                'partial_commit(%d, %d, %r) without pending task'
                % (start_count, end_count, results))

        if not self._work_unit.data['end_count'] == start_count:
            raise ProgrammerError(
                "data['end_count'] = %d != %d = start_count " + \
                'caller failed to start at the right place!?')

        ## update the data
        data = self._work_unit
        data['end_count'] = end_count
        data['results'] += results
        st = make_stream_time()
        data['epoch_ticks'] = st.epoch_ticks
        data['zulu_timestamp'] = st.zulu_timestamp

        ## set the data
        self._work_unit.update()

        data['owner'] = self._worker_id

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

        :param allow_wrong_s3: all i_strs to start with s3:/, which is
        usually wrong, because from_s3_chunks expects paths not URLs
        '''
        count = 0
        start_time = time.time()
        for i_str in i_strs:
            ## ignore leading and trailing whitespace
            i_str = i_str.strip()
            if len(i_str) == 0:
                continue

            if i_str.startswith('s3:/'):
                raise Exception('do not load invalid s3 key strings: %r' % i_str)

            data = self._make_new_data(i_str)

            self._task_master.update_bundle(
                dict(name=self.work_spec_name, min_gb=0, config=self._config),
                {i_str: data})

            count += 1
            if count % 100 == 0:
                elapsed = time.time() - start_time
                rate = float(count) / elapsed
                logger.info('%d in %.f sec --> %.1f tasks/sec' % (count, elapsed, rate))

        return count

    def __len__(self):
        return self._task_master.num_tasks(self.work_spec_name)

    def _len(self, state):
        try:
            val, zstat = self._registry.get(self._path(state))
            return zstat.children_count
        except kazoo.exceptions.NoNodeError:
            return 0

    @property
    def counts(self):
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

