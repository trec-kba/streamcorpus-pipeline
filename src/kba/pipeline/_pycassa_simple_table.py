'''
Provides a simple key=value functionality built on a cassandra.

This software is released under an MIT/X11 open source license.

Copyright 2012-2013 Diffeo, Inc.
'''
import json
import random
import logging
logger = logging.getLogger(__name__)

## get the Cassandra client library
import pycassa
from pycassa.pool import ConnectionPool
from pycassa.system_manager import SystemManager, SIMPLE_STRATEGY, \
    TIME_UUID_TYPE, TIME_UUID_TYPE, ASCII_TYPE, BYTES_TYPE

def _delete_namespace(config):
    sm = SystemManager(config['treelab']['environment']['storage_addresses'][0])
    sm.drop_keyspace(config['treelab']['namespace'])
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
            self.pool = ConnectionPool(namespace, self.server_list)
        except pycassa.InvalidRequestException:
            self._create_namespace(namespace)
            self.pool = ConnectionPool(namespace, self.server_list)

        # Grab a handle on the 'table' column family, creating it if it
        # doesn't already exist
        try:
            self._tasks = pycassa.ColumnFamily(self.pool, 'tasks')
        except pycassa.NotFoundException:
            self._create_column_family('tasks', 
                                       key_validation_class=ASCII_TYPE, 
                                       bytes_columns=['task_data'])
            self._tasks = pycassa.ColumnFamily(self.pool, 'tasks')

        # Grab a handle on the 'meta' column family, creating it if it
        # doesn't already exist
        try:
            self._available = pycassa.ColumnFamily(self.pool, 'available')
        except pycassa.NotFoundException:
            self._create_column_family('available', 
                                        key_validation_class=ASCII_TYPE, 
                                        bytes_columns=['available'])
            self._available = pycassa.ColumnFamily(self.pool, 'available')

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

    @property
    def tasks(self):
        '''
        generate the data objects for every task
        '''
        for row in self._tasks.get_range():
            logger.critical(row)
            yield json.loads(row[1]['task_data'])

    def put_task(self, key, task_data):
        self._tasks.insert(key, {'task_data': json.dumps(task_data)})

    def put_available(self, key):
        ## closest thing to storing only the key
        self._available.insert(key, {'available': ''})

    #def push_batch(self, row_iter):
    #    '''
    #    Push opaque vertex data objects into the inbound queue
    #    '''
    #    return self._tasks.batch_insert({k: json.dumps(v) for k, v in row_iter})

    def get_random_available(self, max_iter=100):
        '''
        get a random key out of the first max_iter rows
        '''
        c = 1
        keeper = None
        for row in self._available.get_range():
            if random.random() < 1 / c:
                keeper = row[0]
            if c == max_iter:
                break
            c += 1
        return keeper

    def close(self):
        self._closed = True
        if hasattr(self, 'pool'):
            self.pool.dispose()
