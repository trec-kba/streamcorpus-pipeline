'''


'''
import time
import uuid
import redis
import random
import socket
import atexit
import logging
import contextlib
from uuid import UUID
from functools import wraps

logger = logging.getLogger('registry')

class EnvironmentError(Exception):
    pass

class LockException(Exception):
    pass

class Registry(object):
    '''provides a centralized storage mechanism for managing groups of
    workers.  These interfaces are defined:

    * state is an application-specific string
    * dictinaries: available, pending, finished

    '''

    def __init__(self, config, renew=False):
        '''
        Initialize the registry using information from the config file
        '''
        self.config = config
        if 'registry_addresses' not in config:
            raise ProgrammerError('registry_addresses not set')
        redis_address, redis_port = config['registry_addresses'][0].split(':')
        redis_port = int(redis_port)
        self._local_ip = self._ipaddress(redis_address, redis_port)

        self._namespace_str = config['namespace']
        self.pool = redis.ConnectionPool(host=redis_address, port=redis_port)
        
        ## populated only when lock is acquired
        self._lock_name = None
        self._session_lock_identifier = None

        if renew:
            self.delete_namespace()
        self._startup()
        atexit.register(self._exit)
        logger.debug('worker_id=%r  starting up on hostname=%r' % (self.worker_id, socket.gethostbyname(socket.gethostname())))

    def _ipaddress(self, host, port):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect((host, port))
        local_ip = s.getsockname()[0]
        s.close()
        return local_ip

    def _startup(self):
        conn = redis.Redis(connection_pool=self.pool)

        ## Get a unique worker id from the registry
        self.worker_id = conn.incrby(self._namespace('worker_ids'), 1)

        ## Add worker id to list of current workers
        conn.sadd(self._namespace('cur_workers'), self.worker_id)

        ## store the local_ip of this worker for debugging
        conn.set(self._namespace(self.worker_id) + '_ip', self._local_ip)

    def _exit(self):
        conn = redis.Redis(connection_pool=self.pool)

        ## cleanup the worker's state
        #with self.lock() as session:
        #    session...

        ## Remove the worker from the list of workers
        conn.srem(self._namespace('cur_workers'), self.worker_id)
        logger.critical('worker_id=%r closed registry client' % self.worker_id)

    def _all_workers(self):
        conn = redis.Redis(connection_pool=self.pool)
        return conn.smembers(self._namespace('cur_workers'))

    def delete_namespace(self):
        '''
        Remove all keys from the namespace
        '''
        conn = redis.Redis(connection_pool=self.pool)
        keys = conn.keys("%s*" % self._namespace_str)
        if keys:
            conn.delete(*keys)

    def _namespace(self, name):
        return "%s_%s" % (self._namespace_str, name)

    def _acquire_lock(self, lock_name, identifier, atime=600, ltime=600):
        '''
        Acquire a lock for a given identifier.  Contend for the lock for atime,
        and hold the lock for ltime.
        '''
        conn = redis.Redis(connection_pool=self.pool)
        end = time.time() + atime
        while end > time.time():
            if conn.set(lock_name, identifier, ex=ltime, nx=True):
                logger.debug("won lock %s" % lock_name)
                return identifier
            sleep_time = random.uniform(0, 3)
            time.sleep(sleep_time)

        logger.warn(
            "failed to acquire lock %s for %f seconds" % (lock_name, atime))
        return False

    def _release_lock(self, lock_name, identifier):
        '''
        This follows a 'test and delete' pattern.
        See redis documentation http://redis.io/commands/set

        This is needed because the lock could have expired before
        we deleted it.  We can only delete the lock if we
        still own it.
        '''
        script = '''
        if redis.call("get", KEYS[1]) == ARGV[1]
        then
            return redis.call("del", KEYS[1])
        else
            return -1
        end
        '''
        conn = redis.Redis(connection_pool=self.pool)
        num_keys_deleted = conn.eval(script, 1, lock_name, identifier)
        if num_keys_deleted == -1:
            ## registry_lock no long owned by this process
            raise EnvironmentError(
                'Lost lock in registry already')
        return True

    @contextlib.contextmanager
    def lock(self, lock_name='global_registry_lock', atime=600, ltime=600):
        '''
        A context manager wrapper around acquring a lock
        '''
        ## Prepend namespace to lock_name
        lock_name = self._namespace(lock_name)

        identifier = str(uuid.uuid4())
        if self._acquire_lock(lock_name, identifier, atime,
                              ltime) != identifier:
            raise LockException("could not acquire lock")
        try:
            self._lock_name = lock_name
            self._session_lock_identifier = identifier
            yield self
        finally:
            self._release_lock(lock_name, identifier)
            self._lock_name = None
            self._session_lock_identifier = None

    def _encode(self, data):
        '''
        Redis hash's store strings in the values of each field.
        Therefore we employ a very simply encoding to turn various
        datatypes into strings.

        Note, that redis would convert things into a string representation
        that we could 'eval' here, but it is not a good practice from a
        security standpoint to eval data from a database.
        '''
        if isinstance(data, tuple):
            return 't:' + '-'.join([self._encode(item) for item in data])
        elif isinstance(data, UUID):
            return 'u:' + str(data.int)
        elif isinstance(data, str):
            return 's:' + data
        elif isinstance(data, int):
            return 'i:' + str(data)
        else:
            logger.error('Fail to encode data %s' % str(data))
            raise TypeError

    def _decode(self, string):
        '''
        Redis hash's store strings in the values of each field.
        Therefore we employ a very simply decoding to turn various
        strings into datatypes we understand.
        '''
        if string[0] == 't':
            return tuple([self._decode(item) for item in
                          string[2:].split('-')])
        elif string[0] == 'u':
            return UUID(int=int(string[2:]))
        elif string[0] == 's':
            return string[2:]
        elif string[0] == 'i':
            return int(string[2:])
        else:
            ## Raise a type error on all other types of data
            raise TypeError

    def update(self, dict_name, mapping):
        '''
        Add mapping to a dictionary, replacing previous values
        '''
        ## script is evaluated with numkeys=2, so KEYS[1] is lock_name,
        ## KEYS[2] is dict_name, ARGV[1] is identifier, and ARGV[i>=2]
        ## are keys and values arranged in pairs.
        script = '''
        if redis.call("get", KEYS[1]) == ARGV[1]
        then
            for i = 2, #ARGV, 2  do
                --if redis.call("hget", KEYS[2], ARGV[i])
                --    then
                --        -- ERROR: dictionary already has key
                --        return -1
                --    end
                redis.call("hset",  KEYS[2], ARGV[i], ARGV[i+1])
                redis.call("rpush", KEYS[2] .. "keys", ARGV[i])
            end
            return 1
        else
            -- ERROR: No longer own the lock
            return 0
        end
        '''
        dict_name = self._namespace(dict_name)
        if self._lock_name is None:
            raise ProgrammerError('must acquire lock first')
        items = []
        ## This flattens the dictionary into a list
        for key, value in mapping.iteritems():
            key = self._encode(key)
            items.append(key)
            value = self._encode(value)
            items.append(value)

        conn = redis.Redis(connection_pool=self.pool)
        res = conn.eval(script, 2, self._lock_name, dict_name, self._session_lock_identifier, *items)
        if not res:
            # We either lost the lock or something else went wrong
            raise EnvironmentError(
                'Unable to add items to %s in registry' % dict_name)

    def pop(self, dict_name, items):
        '''
        Remove items from dictionary.  Analogous to:
        D.pop(k[,d]) -> v, remove specified key and return the corresponding value.
        '''
        ## see comment above for script in update
        script = '''
        if redis.call("get", KEYS[1]) == ARGV[1]
        then
            for i = 2, #ARGV  do
                redis.call("hdel", KEYS[2], ARGV[i])
                redis.call("lrem", KEYS[2] .. "keys", 1, ARGV[i])
            end
            return 1
        else
            -- ERROR: No longer own the lock
            return 0
        end
        '''
        dict_name = self._namespace(dict_name)
        if self._lock_name is None:
            raise ProgrammerError('must acquire lock first')
        conn = redis.Redis(connection_pool=self.pool)
        encoded_items = [self._encode(item) for item in items]
        res = conn.eval(script, 2, self._lock_name, dict_name,
                        self._session_lock_identifier, *encoded_items)
        if len(items) > 0:
            if not res > 0:
                # We either lost the lock or something else went wrong
                raise EnvironmentError(
                    'Unable to remove items from %s in registry: %r' 
                    % (dict_name, res))

    def len(self, dict_name):
        'Length of dictionary'
        dict_name = self._namespace(dict_name)
        conn = redis.Redis(connection_pool=self.pool)
        return conn.hlen(dict_name)

    def popitem(self, dict_name):
        '''
        Pop a (key, value) pair from the dictionary
        '''
        ## see comment above for script in update
        script = '''
        if redis.call("get", KEYS[1]) == ARGV[1]
        then
            -- remove next item of dict_name
            local next_key = redis.call("lpop", KEYS[2] .. "keys")
            local next_val = redis.call("hget", KEYS[2], next_key)
            -- lpop removed it from list, so also remove from hash
            redis.call("hdel", KEYS[2], next_key)
            return {next_key, next_val}
        end
        '''
        dict_name = self._namespace(dict_name)
        if self._lock_name is None:
            raise ProgrammerError('must acquire lock first')
        conn = redis.Redis(connection_pool=self.pool)
        logger.critical('popitem: %s %s %s' 
                        % (self._lock_name, self._session_lock_identifier, dict_name))
        key_value = conn.eval(script, 2, self._lock_name, dict_name, self._session_lock_identifier)
        if not key_value:
            raise KeyError(
                'Registry failed to return an item from %s' % dict_name)

        return self._decode(key_value[0]), self._decode(key_value[1])

    def popitem_move(self, from_dict, to_dict):
        '''
        Pop an item out of from_dict, store it in to_dict, and return it
        '''
        script = '''
        if redis.call("get", KEYS[1]) == ARGV[1]
        then
            -- remove next item of from_dict
            local next_key = redis.call("lpop", KEYS[2] .. "keys")
            local next_val = redis.call("hget", KEYS[2], next_key)
            -- lpop removed it from list, so also remove from hash
            redis.call("hdel", KEYS[2], next_key)

            -- put it in to_dict
            redis.call("hset",  KEYS[3], next_key, next_val)
            redis.call("rpush", KEYS[3] .. "keys", next_key)

            return {next_key, next_val}
        end
        '''
        if self._lock_name is None:
            raise ProgrammerError('must acquire lock first')
        conn = redis.Redis(connection_pool=self.pool)
        key_value = conn.eval(script, 3, self._lock_name, 
                              self._namespace(from_dict), 
                              self._namespace(to_dict), 
                              self._session_lock_identifier)
        if not key_value:
            raise KeyError(
                'Registry.pop_move failed to return an item from %s' % from_dict)

        return self._decode(key_value[0]), self._decode(key_value[1])

    def move(self, from_dict, to_dict, mapping):
        '''
        Pop mapping out of from_dict, and update them in to_dict
        '''
        script = '''
        if redis.call("get", KEYS[1]) == ARGV[1]
        then
            local count = 0
            for i = 2, #ARGV, 2  do
                -- remove next item of from_dict
                local next_key = redis.call("lpop", KEYS[2] .. "keys")
                local next_val = redis.call("hget", KEYS[2], next_key)
                -- lpop removed it from list, so also remove from hash
                redis.call("hdel", KEYS[2], next_key)
                -- put it in to_dict
                redis.call("hset",  KEYS[3], ARGV[i], ARGV[i+1])
                redis.call("rpush", KEYS[3] .. "keys", ARGV[i])
                count = count + 1
            end
            return count
        else
            -- ERROR: No longer own the lock
            return 0
        end
        '''
        if self._lock_name is None:
            raise ProgrammerError('must acquire lock first')
        items = []
        ## This flattens the dictionary into a list
        for key, value in mapping.iteritems():
            key = self._encode(key)
            items.append(key)
            value = self._encode(value)
            items.append(value)

        conn = redis.Redis(connection_pool=self.pool)

        num_moved = conn.eval(script, 3, self._lock_name, 
                              self._namespace(from_dict), 
                              self._namespace(to_dict), 
                              self._session_lock_identifier, *items)
        if num_moved != len(items) / 2:
            raise EnvironmentError(
                'Registry failed to move all: num_moved = %d != %d len(items)'
                % (num_moved, len(items)))


    def pull(self, dict_name):
        '''
        Pull entire dictionary
        '''
        dict_name = self._namespace(dict_name)
        conn = redis.Redis(connection_pool=self.pool)
        res = conn.hgetall(dict_name)
        split_res = {self._decode(key):
                     self._decode(value)
                     for key, value in res.iteritems()}
        return split_res

    def _conn(self):
        '''
        debugging aid to easily grab a connection from the connection pool
        '''
        conn = redis.Redis(connection_pool=self.pool)
        return conn

    def incrby(self, key, value=1):
        conn = redis.Redis(connection_pool=self.pool)
        conn.incrby(self._namespace(key), value)

    def getkey(self, key):
        conn = redis.Redis(connection_pool=self.pool)
        return conn.get(self._namespace(key))

    def set_state(self, state):
        conn = redis.Redis(connection_pool=self.pool)
        conn.set(self._namespace('state'), state)

    def get_state(self):
        conn = redis.Redis(connection_pool=self.pool)
        return conn.get(self._namespace('state'))

    def direct_call(self, *args):
        ## Note this is temporary until we understand what functions we
        ## actually want to expose
        conn = redis.Redis(connection_pool=self.pool)
        command = args[0]
        key = self._namespace(args[1])
        args = args[2:]
        func = getattr(conn, command)
        return func(key, *args)


if __name__ == '__main__':
    pass
