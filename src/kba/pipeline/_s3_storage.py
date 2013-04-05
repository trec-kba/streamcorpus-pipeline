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
import logging
import hashlib
import requests
import traceback
import _extract_spinn3r
import streamcorpus
from _get_name_info import get_name_info
from streamcorpus import decrypt_and_uncompress, compress_and_encrypt_path, Chunk
from cStringIO import StringIO
from _exceptions import FailedExtraction

logger = logging.getLogger(__name__)

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

            except OSError, exc:
                ## OSError: [Errno 24] Too many open files
                raise exc

            except FailedExtraction, exc:
                ## pass through exc to caller
                raise exc

            except Exception, exc:
                time.sleep(3 * tries)
                msg = 'FAIL(%d): having I/O trouble with S3: %s' % \
                    (tries, traceback.format_exc(exc))
                logger.info(msg)
                tries += 1
                if tries > self.config['tries']:
                    ## indicate complete failure to pipeline so it
                    ## gets recorded in task_queue
                    raise FailedExtraction(msg)
                    
    return retry_func

def get_bucket(config):
    ## use special keys for accessing AWS public data sets bucket
    aws_access_key_id =     open(config['aws_access_key_id_path']).read()
    aws_secret_access_key = open(config['aws_secret_access_key_path']).read()
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
        s3://<bucket><s3_prefix_path>/data_hour/
        '''
        logger.info('from_s3_chunks: %r' % i_str)
        key = Key(self.bucket, i_str.strip())
        return self.get_chunk(key)

    @_retry
    def get_keys(self, date_hour):
        '''
        Given a date_hour dir, generate all the Key instances for
        chunks in this dir, requires fetch, decrypt, uncompress,
        deserialize:
        '''
        prefix = os.path.join(self.config['s3_path_prefix'], date_hour)
        return self.bucket.list(prefix=prefix)

    @_retry
    def get_chunk(self, key):
        tries = 0
        while 1:
            fh = StringIO()
            key.get_contents_to_file(fh)
            data = fh.getvalue()
            _errors, data = decrypt_and_uncompress(
                data, 
                self.config['gpg_decryption_key_path'])
            logger.info( '\n'.join(_errors) )
            if self.config['input_format'] == 'streamitem' and \
                    self.config['streamcorpus_version'] == 'v0_1_0':
                i_content_md5 = key.key.split('.')[-3]
            else:
                ## go past {sc,protostream}.xz.gpg
                i_content_md5 = key.key.split('.')[-4][-32:]

            ## verify the data matches expected md5
            f_content_md5 = hashlib.md5(data).hexdigest()
            if i_content_md5 != f_content_md5:
                msg = 'FAIL(%d): %s --> %s != %s' % (tries, key.key, i_content_md5, f_content_md5)
                logger.critical(msg)
                tries += 1
                if tries > self.config['tries']:
                    ## indicate complete failure to pipeline so it
                    ## gets recorded in task_queue
                    raise FailedExtraction(msg)
                else:
                    continue

            if self.config['input_format'] == 'spinn3r':
                ## convert the data from spinn3r's protostream format
                return _extract_spinn3r._generate_stream_items( data )

            elif self.config['input_format'] == 'streamitem':
                message = _message_versions[ self.config['streamcorpus_version'] ]

                return streamcorpus.Chunk(data=data, message=message)

            else:
                sys.exit('Invalid config: input_format = %r' % self.config['input_format'])

class to_s3_chunks(object):
    def __init__(self, config):
        self.config = config
        self.bucket = get_bucket(config)

    def __call__(self, t_path, name_info, i_str):
        '''
        Load chunk from t_path and put it into the right place in s3
        using the output_name template from the config
        '''
        name_info.update( get_name_info(t_path) )
        if name_info['num'] == 0:
            o_path = None
            return o_path

        o_fname = self.config['output_name'] % name_info
        o_path = os.path.join(self.config['s3_path_prefix'], o_fname + '.sc.xz.gpg')

        logger.info('to_s3_chunks: \n\t%r\n\tfrom: %r\n\tby way of %r ' % (o_path, i_str, t_path))

        ## forcibly collect dereferenced objects
        #gc.collect()

        ## compress and encrypt
        logger.critical( 'key path: %r' % self.config['gpg_encryption_key_path'] )
        _errors, t_path2 = compress_and_encrypt_path(
            t_path, 
            self.config['gpg_encryption_key_path'],
            gpg_recipient=self.config['gpg_recipient'])
        logger.info( '\n'.join(_errors) )

        data = open(t_path2).read()
        logger.debug('compressed size: %d' % len(data))
        while 1:
            start_time  = time.time()
            self.put(o_path, data)
            elapsed = time.time() - start_time
            if elapsed  > 0:
                logger.debug('put %.1f bytes/second' % (len(data) / elapsed))

            if self.config['verify_via_http']:
                try:
                    start_time = time.time()
                    self.verify(o_path, name_info['md5'])
                    elapsed = time.time() - start_time
                    if elapsed > 0:
                        logger.debug('verify %.1f bytes/second' % (len(data) / elapsed))

                    break
                except Exception, exc:
                    logger.critical( 'verify_via_http failed so retrying: %r' % exc )
                    ## keep looping if verify raises anything
                    continue

            else:
                ## not verifying, so don't attempt multiple puts
                break

        logger.info('to_s3_chunks finished: %r' % i_str)
        if self.config['cleanup_tmp_files']:
            try:
                os.remove( t_path )
            except Exception, exc:
                logger.info('%s --> failed to remove %s' % (exc, t_path))

        ## return the final output path
        return o_path

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
        logger.info('fetching %r' % url)
        req = requests.get(url)
        errors, data = decrypt_and_uncompress(
            req.content, 
            self.config['gpg_decryption_key_path'])

        logger.info( 'got back SIs: %d' % len( list( Chunk(data=data) ) ))

        rec_md5 = hashlib.md5(data).hexdigest()
        if md5 == rec_md5:
            return
        else:
            logger.critical('\n'.join(errors))
            raise Exception('original md5 = %r != %r = received md5' % (md5, rec_md5))
