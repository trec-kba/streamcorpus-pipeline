

import os
import socket
import hashlib
import getpass


def make_namespace_string(test_name='test'):
    '''
    generates a descriptive namespace for testing, which is unique to
    this user and also this process ID and host running the test.

    This also ensures that the namespace name is shorter than 48 chars
    and obeys the other constraints of the various backend DBs that we
    use.
    '''
    return '_'.join([
            test_name[:25], 
            getpass.getuser().replace('-', '_')[:5],
            #str(os.getpid()),
            hashlib.md5(socket.gethostname()).hexdigest()[:4],
            ])
