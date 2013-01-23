#!/usr/bin/env python
'''
Provides classes for loading chunk files from local storage and
putting them out into local storage.

This software is released under an MIT/X11 open source license.

Copyright 2012 Diffeo, Inc.
'''
import os
import sys
import time
import hashlib
import traceback
import streamcorpus
from streamcorpus import decrypt_and_uncompress, compress_and_encrypt
from cStringIO import StringIO

_message_versions = {
    'v0_1_0': streamcorpus.StreamItem_v0_1_0,
    'v0_2_0': streamcorpus.StreamItem,
    }

import boto
from boto.s3.key import Key
from boto.s3.connection import S3Connection
## stop Boto's built-in retries, so we can do our own
if not boto.config.has_section('Boto'):
    boto.config.add_section('Boto')
boto.config.set('Boto', 'num_retries', '0')

def _retry(func):
    '''
    Decorator for methods that need many retries, because of
    intermittent failures, such as AWS calls via boto, which has a
    non-back-off retry.
    '''
    def retry_func(self, *args, **kwargs):
        tries = 0
        while 1:
            try:
                return func(self, *args, **kwargs)
                break
            except Exception, exc:
                time.sleep(3 * tries)
                print('having trouble writing: %s' % traceback.format_exc(exc))
                tries += 1
                if tries > self.config['tries']:
                    log('giving up on uploading: %s' % output_path)
                    sys.exit('giving up on uploading: %s' % output_path)
    return retry_func

class from_s3_chunks(object):
    def __init__(self, config):
        self.config = config
        ## use special keys for accessing AWS public data sets bucket
        conn = S3Connection(self.config['aws_access_key_id'],
                            self.config['aws_secret_access_key'])
        self.bucket = conn.get_bucket(self.config['bucket'])

    def __call__(self, i_str):
        '''
        Takes a date_hour string over stdin and generates chunks from
        s3://<bucket><prefix_path>/data_hour/
        '''
        date_hour = i_str.strip()
        print date_hour
        for key in self.get_keys(date_hour):
            print key.key
            if key.key.endswith('xz.gpg'):
                yield self.get_chunk(key)

    @_retry
    def get_keys(self, date_hour):
        '''
        Given a date_hour dir, generate all the Key instances for
        chunks in this dir, requires fetch, decrypt, uncompress,
        deserialize:
        '''
        prefix = os.path.join(self.config['path_prefix'], date_hour)
        return self.bucket.list(prefix=prefix)

    @_retry
    def get_chunk(self, key):
        fh = StringIO()
        key.get_contents_to_file(fh)
        data = fh.getvalue()
        _errors, data = decrypt_and_uncompress(data, self.config['gpg_key'], self.config['gpg_dir'])
        print '\n'.join(_errors)
        i_content_md5 = key.key.split('.')[-3]
        f_content_md5 = hashlib.md5(data).hexdigest()
        if i_content_md5 != f_content_md5:
            msg = 'FAIL: %s --> %s' % (key.key, f_content_md5)
            print(msg)
            sys.exit(msg)

        message = _message_versions[ self.config['streamcorpus_version'] ]

        return streamcorpus.Chunk(data=data, message=message)

class to_s3_chunks(object):
    def __init__(self, config):
        self.config = config
        ## use special keys for accessing AWS public data sets bucket
        conn = S3Connection(self.config['aws_access_key_id'],
                            self.config['aws_secret_access_key'])
        self.bucket = conn.get_bucket(self.config['bucket'])

    def __call__(self, t_path, name_info, i_str):
        date_hour = i_str.strip()
        name_info['date_hour'] = date_hour
        o_fname = self.config['output_name'] % name_info
        o_path = os.path.join(self.config['path_prefix'], o_fname + '.sc.xz.gpg')
        print o_path

        data = open(t_path).read()
        _errors, data = compress_and_encrypt(data, self.config['gpg_key'], self.config['gpg_dir'])
        print '\n'.join(_errors)

        self.put(o_path, data)

    @_retry
    def put(self, o_path, data):
        key = Key(self.bucket, o_path)
        key.set_contents_from_file(StringIO(data))
        key.set_acl('public-read')
