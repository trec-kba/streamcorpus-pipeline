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
import requests
import traceback
import _extract_spinn3r
import streamcorpus
from streamcorpus import decrypt_and_uncompress, compress_and_encrypt, Chunk
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
                print('having I/O trouble with S3: %s' % traceback.format_exc(exc))
                tries += 1
                if tries > self.config['tries']:
                    sys.exit('giving up')
    return retry_func

def get_bucket(config):
    ## use special keys for accessing AWS public data sets bucket
    aws_access_key_id = open(config['aws_access_key_id']).read()
    aws_secret_access_key = open(config['aws_secret_access_key']).read()
    conn = S3Connection(aws_access_key_id,
                        aws_secret_access_key)
    bucket = conn.get_bucket(config['bucket'])
    return bucket

class from_s3_chunks(object):
    def __init__(self, config):
        self.config = config
        self.bucket = get_bucket(config)

    def __call__(self, i_str):
        '''
        Takes a date_hour string over stdin and generates chunks from
        s3://<bucket><prefix_path>/data_hour/
        '''
        print('from_s3_chunks: %r' % i_str)
        if self.config['task_type'] == 'date_hour':
            date_hour = i_str.strip()

            for key in self.get_keys(date_hour):
                print key.key
                if key.key.endswith('xz.gpg'):
                    yield self.get_chunk(key)

        elif self.config['task_type'] == 'full_path':
            key = Key(self.bucket, i_str.strip())
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
        _errors, data = decrypt_and_uncompress(data, self.config['gpg_decryption_key'], self.config['gpg_dir'])
        print '\n'.join(_errors)
        if self.config['input_format'] == 'streamitem' and \
                self.config['streamcorpus_version'] == 'v0_1_0':
            i_content_md5 = key.key.split('.')[-3]
        else:
            ## go past {sc,protostream}.xz.gpg
            i_content_md5 = key.key.split('.')[-4]
        f_content_md5 = hashlib.md5(data).hexdigest()
        if i_content_md5 != f_content_md5:
            msg = 'FAIL: %s --> %s' % (key.key, f_content_md5)
            print(msg)
            sys.exit(msg)

        if self.config['input_format'] == 'spinn3r':
            ## convert the data from spinn3r's protostream format
            return _extract_spinn3r._generate_stream_items( data )

        elif self.config['input_format'] == 'streamcorpus':
            message = _message_versions[ self.config['streamcorpus_version'] ]

            return streamcorpus.Chunk(data=data, message=message)

        else:
            sys.exit('Invalid config: input_format = %r' % self.config['input_format'])

def log(mesg):
    print(mesg)
    sys.stdout.flush()

class to_s3_chunks(object):
    def __init__(self, config):
        self.config = config
        self.bucket = get_bucket(config)

    def __call__(self, t_path, name_info, i_str):
        '''
        Load chunk from t_path and put it into the right place in s3
        using the output_name template from the config
        '''
        data = open(t_path).read()
        log('got %d bytes from file' % len(data))

        ch = Chunk(data=data)
        date_hours = set()
        for si in ch:
            date_hours.add( si.stream_time.zulu_timestamp[:13] )

        ## create the md5 property, so we can use it in the filename
        name_info['md5'] = ch.md5_hexdigest

        assert len(date_hours) == 1, \
            'got a chunk with other than one data_hour! ' + \
            repr(date_hours)

        date_hour = list(date_hours)[0]
        date_hour = date_hour.replace('T', '-')
                
        if self.config['task_type'] == 'date_hour':
            expected_date_hour = i_str.strip()
            assert date_hour == expected_date_hour, \
                (date_hour, expected_date_hour)

        name_info['date_hour'] = date_hour
        o_fname = self.config['output_name'] % name_info
        o_path = os.path.join(self.config['path_prefix'], o_fname + '.sc.xz.gpg')

        log('to_s3_chunks: %r\nfrom: %r\n by way of %r ' % (o_path, i_str, t_path))

        ## compress and encrypt
        _errors, data = compress_and_encrypt(
            data, self.config['gpg_encryption_key'], self.config['gpg_dir'])
        print '\n'.join(_errors)

        log('compressed size: %d' % len(data))
        while 1:
            start_time  = time.time()
            self.put(o_path, data)
            elapsed = time.time() - start_time
            if elapsed  > 0:
                log('put %.1f bytes/second' % (len(data) / elapsed))

            if self.config['verify_via_http']:
                try:
                    start_time = time.time()
                    self.verify(o_path, name_info['md5'])
                    elapsed = time.time() - start_time
                    if elapsed > 0:
                        log('verify %.1f bytes/second' % (len(data) / elapsed))

                    break
                except Exception, exc:
                    print 'verify_via_http failed so retrying: %r' % exc
                    ## keep looping if verify raises anything
                    continue

            else:
                ## not verifying, so don't attempt multiple puts
                break

        print('to_s3_chunks: %r' % i_str)

    @_retry
    def put(self, o_path, data):
        key = Key(self.bucket, o_path)
        key.set_contents_from_file(StringIO(data))
        key.set_acl('public-read')

    @_retry
    def verify(self, o_path, md5):
        url = 'http://s3.amazonaws.com/%(bucket)s/%(o_path)s' % dict(
            bucket = self.config['bucket'],
            o_path = o_path)
        print('fetching %r' % url)
        req = requests.get(url)
        errors, data = decrypt_and_uncompress(
            req.content, 
            self.config['gpg_decryption_key'], self.config['gpg_dir'])

        print 'got back SIs: %d' % len( list( Chunk(data=data) ) )

        rec_md5 = hashlib.md5(data).hexdigest()
        if md5 == rec_md5:
            return
        else:
            print errors
            raise Exception('original md5 = %r != %r = received md5' % (md5, rec_md5))
