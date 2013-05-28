'''
Provides a simple key=value functionality built on a cassandra.

This software is released under an MIT/X11 open source license.

Copyright 2012-2013 Diffeo, Inc.
'''
import json
import random
import hashlib
import logging
logger = logging.getLogger(__name__)

## get the Cassandra client library
import pycassa
from pycassa.pool import ConnectionPool
from pycassa.system_manager import SystemManager, SIMPLE_STRATEGY, \
    TIME_UUID_TYPE, TIME_UUID_TYPE, ASCII_TYPE, BYTES_TYPE, \
    COUNTER_COLUMN_TYPE
from pycassa.types import CounterColumnType, UTF8Type

def _delete_namespace(config):
    sm = SystemManager(config['storage_addresses'][0])
    sm.drop_keyspace(config['namespace'])
    sm.close()

class Cassa(object):
    '''
    Provides a simple key=value functionality built on a cassandra
    table with a key and a single column.  Used in ZookeeperTaskQueue
    to replace the two tables that grow in size with the number of
    tasks rather than the number of workers.
    '''

    def __init__(self, namespace, server_list=['localhost:9160']):
        # save cassandra server
        self.server_list = server_list
        self.namespace = namespace
        self._closed = False

        #setup_logging(self)

        # Connect to the server creating the namespace if it doesn't
        # already exist
        try:
            self.pool = ConnectionPool(namespace, self.server_list, max_retries=500, pool_timeout=600, timeout=10)
        except pycassa.InvalidRequestException:
            self._create_namespace(namespace)
            self.pool = ConnectionPool(namespace, self.server_list, max_retries=500, pool_timeout=600, timeout=10)

        try:
            self._tasks = pycassa.ColumnFamily(self.pool, 'tasks')
        except pycassa.NotFoundException:
            self._create_column_family('tasks', 
                                       key_validation_class=ASCII_TYPE, 
                                       bytes_columns=['task_data'])
            self._tasks = pycassa.ColumnFamily(self.pool, 'tasks')

        try:
            self._available = pycassa.ColumnFamily(self.pool, 'available')
        except pycassa.NotFoundException:
            self._create_column_family('available', 
                                        key_validation_class=ASCII_TYPE, 
                                        bytes_columns=['available'])
            self._available = pycassa.ColumnFamily(self.pool, 'available')

        try:
            self._task_count = pycassa.ColumnFamily(self.pool, 'task_count')
        except pycassa.NotFoundException:
            self._create_counter_column_family('task_count', 
                                       key_validation_class=ASCII_TYPE, 
                                       counter_columns=['task_count'])
            self._task_count = pycassa.ColumnFamily(self.pool, 'task_count')
            self._task_count.insert('RowKey', {'task_count': 0})

        try:
            self._available_count = pycassa.ColumnFamily(self.pool, 'available_count')
        except pycassa.NotFoundException:
            self._create_counter_column_family('available_count', 
                                       key_validation_class=ASCII_TYPE, 
                                       counter_columns=['available_count'])
            self._available_count = pycassa.ColumnFamily(self.pool, 'available_count')
            self._available_count.insert('RowKey', {'available_count': 0})

    def delete_namespace(self):
        sm = SystemManager(random.choice(self.server_list))
        sm.drop_keyspace(self.namespace)
        sm.close()

    def _create_namespace(self, namespace):
        sm = SystemManager(random.choice(self.server_list))
        sm.create_keyspace(namespace, SIMPLE_STRATEGY, {'replication_factor': '1'})
        sm.close()

    def _create_column_family(self, family, bytes_columns=[], 
                              key_validation_class=TIME_UUID_TYPE):
        '''
        Creates a column family of the name 'family' and sets any of
        the names in the bytes_column list to have the BYTES_TYPE.

        key_validation_class defaults to TIME_UUID_TYPE and could also
        be ASCII_TYPE for md5 hash keys, like we use for 'inbound'
        '''
        sm = SystemManager(random.choice(self.server_list))
        # sys.create_column_family(self.namespace, family, super=False)
        sm.create_column_family(self.namespace, family, super=False,
                key_validation_class = key_validation_class, 
                default_validation_class  = TIME_UUID_TYPE,
                column_name_class = ASCII_TYPE)
        for column in bytes_columns:
            sm.alter_column(self.namespace, family, column, BYTES_TYPE)
        sm.close()

    def _create_counter_column_family(self, family, counter_columns=[],
                              key_validation_class=UTF8Type):
        '''
        Creates a column family of the name 'family' and sets any of
        the names in the bytes_column list to have the BYTES_TYPE.

        key_validation_class defaults to TIME_UUID_TYPE and could also
        be ASCII_TYPE for md5 hash keys, like we use for 'inbound'
        '''
        sm = SystemManager(random.choice(self.server_list))
        # sys.create_column_family(self.namespace, family, super=False)
        sm.create_column_family(self.namespace, family, super=False,
                key_validation_class = key_validation_class, 
                default_validation_class="CounterColumnType",
                column_name_class = ASCII_TYPE)
        for column in counter_columns:
            sm.alter_column(self.namespace, family, column, COUNTER_COLUMN_TYPE)
        sm.close()

    def tasks(self, key_prefix=''):
        '''
        generate the data objects for every task
        '''
        for row in self._tasks.get_range():
            logger.debug(row)
            if not row[0].startswith(key_prefix):
                continue
            data = json.loads(row[1]['task_data'])
            data['task_key'] = row[0]
            yield data

    def put_task(self, key, task_data):
        try:
            found = self._tasks.get(key, column_count=1)
            exists = True
        except pycassa.cassandra.ttypes.NotFoundException:
            exists = False

        self._tasks.insert(key, {'task_data': json.dumps(task_data)})
        if not exists:
            self._task_count.insert('RowKey', {'task_count': 1})
        return exists

    def get_task(self, key):
        data = self._tasks.get(key)
        return json.loads(data['task_data'])

    def pop_task(self, key):
        self._tasks.remove(key)
        self._task_count.insert('RowKey', {'task_count': -1})
        return key

    @property
    def task_keys(self):
        c = 0
        for key, _ in self._tasks.get_range(column_count=0, filter_empty=False):
            c += 1
            yield key

    def num_tasks(self):
        data = self._task_count.get('RowKey')
        return data['task_count']

    def num_available(self):
        data = self._available_count.get('RowKey')
        return data['available_count']

    def put_available(self, key):
        ## closest thing to storing only the key
        try:
            found = self._available.get(key, column_count=1)
            exists = True
        except pycassa.cassandra.ttypes.NotFoundException:
            exists = False

        if not exists:
            self._available.insert(key, {'available': ''})
            self._available_count.insert('RowKey', {'available_count': 1})

    #def push_batch(self, row_iter):
    #    '''
    #    Push opaque vertex data objects into the inbound queue
    #    '''
    #    return self._tasks.batch_insert({k: json.dumps(v) for k, v in row_iter})

    def get_random_available(self, max_iter=10000):
        '''
        get a random key out of the first max_iter rows
        '''
        c = 1
        keeper = None
        ## note the ConsistencyLevel here.  If we do not do this, and
        ## get all slick with things like column_count=0 and filter
        ## empty False, then we can get keys that were recently
        ## deleted... EVEN if the default consistency would seem to
        ## rule that out!

        ## note the random start key, so that we do not always hit the
        ## same place in the key range with all workers
        #random_key = hashlib.md5(str(random.random())).hexdigest()
        #random_key = '0' * 32
        #logger.debug('available.get_range(%r)' % random_key)
        ## scratch that idea: turns out that using a random start key
        ## OR using row_count=1 can cause get_range to hang for hours

        ## why we need ConsistencyLevel.ALL on a single node is not
        ## clear, but experience indicates it is needed.

        ## note that putting a finite row_count is problematic in two
        ## ways:
        # 1) if there are more workers than max_iter, some will not
        # get tasks
        #
        # 2) if there are more than max_iter records, then all workers
        # have to wade through all of these just to get a task!  What
        # we really want is a "pick random row" function, and that is
        # probably best implemented using CQL3 token function via the
        # cql python module instead of pycassa...
        for row in self._available.get_range(row_count=max_iter, read_consistency_level=pycassa.ConsistencyLevel.ALL):
        #for row in self._available.get_range(row_count=100):
            logger.debug('considering %r' % (row,))
            if random.random() < 1 / c:
                keeper = row[0]
            if c == max_iter:
                break
            c += 1
        return keeper

    def in_available(self, key):
        try:
            row = self._available.get(key)
            return True
        except pycassa.NotFoundException:
            return False

    def pop_available(self, key):
        self._available.remove(key, write_consistency_level=pycassa.ConsistencyLevel.ALL)
        self._available_count.insert('RowKey', {'available_count': -1})
        assert not self.in_available(key)
        return key

    def close(self):
        self._closed = True
        if hasattr(self, 'pool'):
            self.pool.dispose()
