#!/usr/bin/env python
'''
Provides classes for loading chunk files from local storage and
putting them out into local storage.

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2014 Diffeo, Inc.
'''
from __future__ import absolute_import, division, print_function
from cStringIO import StringIO
from functools import partial
import gzip
import hashlib
import logging
import os
import re
import sys
import time

import requests

try:
    from dossier.fc import FeatureCollectionChunk as FCChunk
except ImportError:
    FCChunk = None

import streamcorpus
from streamcorpus import decrypt_and_uncompress, \
    parse_file_extensions, known_compression_schemes, \
    compress_and_encrypt_path, Chunk
from streamcorpus_pipeline._exceptions import FailedExtraction, \
    FailedVerification, ConfigurationError
from streamcorpus_pipeline._get_name_info import get_name_info
from streamcorpus_pipeline._spinn3r_feed_storage import _generate_stream_items
from streamcorpus_pipeline.stages import Configured
from streamcorpus_pipeline._tarball_export import tarball_export

logger = logging.getLogger(__name__)

_message_versions = {
    'v0_1_0': streamcorpus.StreamItem_v0_1_0,
    'v0_2_0': streamcorpus.StreamItem_v0_2_0,
    'v0_3_0': streamcorpus.StreamItem_v0_3_0,
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
        tries = 1
        while True:
            # If a handler allows execution to continue, then
            # fall through and do a back-off retry.
            try:
                return func(self, *args, **kwargs)
                break
            except OSError as exc:
                ## OSError: [Errno 24] Too many open files
                logger.error('assuming OSError unrecoverable')
                raise
            except FailedExtraction as exc:
                ## pass through exc to caller
                logger.error('FAIL(%d)', tries, exc_info=True)
                raise
            except FailedVerification as exc:
                logger.warn('FAIL(%d)', tries, exc_info=True)
                if tries >= self.config['tries']:
                    if self.config.get('suppress_failures'):
                        logger.warn('suppressing failure and breaking out of this loop; data may be corrupt, downstream will have to cope')
                        break
                    else:
                        raise
            except Exception as exc:
                logger.warn('FAIL(%d): having I/O trouble with S3', tries, exc_info=True)
                if tries >= self.config['tries']:
                    raise

            logger.warn('RETRYING (%d left)', self.config['tries'] - tries)
            time.sleep(3 * tries)
            tries += 1

    return retry_func


def timedop(what, datalen, fun):
    start_time = time.time()
    try:
        retval = fun()
    except:
        elapsed = time.time() - start_time
        if elapsed  > 0:
            logger.debug('%s %.1f bytes/second ERROR', what, float(datalen) / float(elapsed), exc_info=True)
        raise

    elapsed = time.time() - start_time
    if elapsed  > 0:
        logger.debug('%s %.1f bytes/second', what, float(datalen) / float(elapsed))
    return retval


def verify_md5(md5_expected, data, other_errors=None):
    "return True if okay, raise Exception if not"  # O_o ?
    md5_recv = hashlib.md5(data).hexdigest()
    if md5_expected != md5_recv:
        if other_errors is not None:
            logger.critical('\n'.join(other_errors))
        raise FailedVerification('original md5 = %r != %r = received md5' \
                                 % (md5_expected, md5_recv))
    return True


def get_bucket(config, bucket_name=None):
    '''This function is mostly about managing configuration, and then
    finally returns a boto.Bucket object.

    AWS credentials come first from config keys
    aws_access_key_id_path, aws_secret_access_key_path (paths to one
    line files); secondly from environment variables
    AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY.

    '''
    if not bucket_name:
        if 'bucket' not in config:
            raise ConfigurationError(
                'The "bucket" parameter is required for the s3 stages.')
        bucket_name = config['bucket']

    # get AWS credentials. first, from config; else, from env vars.
    aws_access_key_id_path = config.get('aws_access_key_id_path')
    aws_secret_access_key_path = config.get('aws_secret_access_key_path')

    if (not aws_access_key_id_path) and (not aws_secret_access_key_path):
        logger.debug('using aws credentials from environment')
        access = os.getenv('AWS_ACCESS_KEY_ID')
        secret = os.getenv('AWS_SECRET_ACCESS_KEY')
        if (not access) or (not secret):
            msg = 'aws credentials not configured in aws_access_key_id_path+aws_secret_access_key_path, and not available in environment AWS_ACCESS_KEY_ID+AWS_SECRET_ACCESS_KEY'
            logger.error(msg)
            raise Exception(msg)
    else:
        try:
            access = open(aws_access_key_id_path).read().strip()
            secret = open(aws_secret_access_key_path).read().strip()
        except:
            logger.error('failed reading aws credentials from configured file', exc_info=True)
            raise

    conn = S3Connection(access, secret)
    bucket = conn.get_bucket(bucket_name)
    return bucket


class from_s3_chunks(Configured):
    '''
    Reads data from Amazon S3 one key at a time. The type of data read
    can be specified with the ``input_format`` config option. The
    following values are legal: ``streamitem``, ``featurecollection``
    or ``spinn3r``.

    When the input format is ``streamitem`` or ``spinn3r``, then this
    reader produces a generator of :class:`streamcorpus.StreamItem`
    instances.

    When the input format is ``featurecollection``, then this reader
    produces a generator of ``dossier.fc.FeatureCollection`` instances.

    ``bucket`` is the s3 bucket to use if input paths are not full
    s3://{bucket}{path} URIs. ``aws_access_key_id_path`` and
    ``aws_secret_access_key_path`` should point to files containing
    your s3 credentials. Alternatley credentials can be in environment
    variables ```AWS_ACCESS_KEY_ID`` and ``AWS_SECRET_ACCESS_KEY``

    The rest of the configuration options are optional and are described
    in the following example:

    .. code-block:: yaml

        from_s3_chunks:
          # How to connect to S3
          # bucket can also come from input path if full uri s3://{bucket}{path}
          bucket: aws-publicdatasets
          # can also come from environment variable AWS_ACCESS_KEY_ID
          aws_access_key_id_path: keys/aws_access_key_id
          # can also come from environment variable AWS_SECRET_ACCESS_KEY
          aws_secret_access_key_path: keys/aws_secret_access_key

          # Optional parameters.
          
          # The number of times to try reading from s3. A value of
          # `1` means the download is tried exactly once.
          # The default value is `10`.
          tries: 1

          # This is prepended to every key given. Namely, all s3 URLs
          # are formed by `s3://{bucket}/{s3_path_prefix}/{input-key-name}`.
          # By default, it is empty.
          s3_path_prefix: some/s3/prefix

          # A path to a private GPG decryption key file.
          gpg_decryption_key_path: keys/gpg-key

          # The type of data to read from s3. Valid values are
          # "StreamItem", "spinn3r" or "FeatureCollection".
          # The default is "StreamItem".
          input_format: StreamItem

          # When the input format is "StreamItem", this indicates the
          # Thrift version to use. Defaults to "v0_3_0".
          streamcorpus_version: v0_3_0

          # When set, an md5 is extracted from the s3 key and is used
          # to verify the decrypted and decompressed content downloaded.
          # This is disabled by default.
          compare_md5_in_file_name: true

          # A temporary directory where intermediate files may reside.
          # Uses your system's default tmp directory (usually `/tmp`)
          # by default.
          tmp_dir_path: /tmp
    '''
    config_name = 'from_s3_chunks'
    default_config = {
        'compare_md5_in_file_name': False,
        's3_path_prefix': '',
        'tmp_dir_path': None,
        'tries': 10,
        'gpg_decryption_key_path': None,
        'input_format': 'StreamItem',
        'streamcorpus_version': 'v0_3_0',
    }

    def __init__(self, config):
        super(from_s3_chunks, self).__init__(config)
        self.gpg_decryption_key_path = config.get('gpg_decryption_key_path')

    def __call__(self, i_str):
        '''
        Takes a path suffix as a string over stdin and generates chunks from
        s3://<bucket><s3_prefix_path>/{i_str}.
        '''
        if i_str.startswith('s3://'):
            # full path including bucket name
            bucket_name, kpath = i_str[5:].split('/', 1)
            assert bucket_name and kpath, 'bad s3 url {0!r}'.format(i_str)
        else:
            # get bucket_name from config
            bucket_name = None
            kpath = os.path.join(self.config['s3_path_prefix'].strip(),
                                 i_str.strip())
        logger.info('from_s3_chunks: %s / %r', bucket_name, kpath)
        return self.get_chunk(bucket_name, kpath)

    def _decode(self, data):
        '''
        Given the raw data from s3, return a generator for the items
        contained in that data. A generator is necessary to support
        chunk files, but non-chunk files can be provided by a generator
        that yields exactly one item.

        Decoding works by case analysis on the config option
        ``input_format``. If an invalid ``input_format`` is given, then
        a ``ConfigurationError`` is raised.
        '''
        informat = self.config['input_format'].lower()
        if informat == 'spinn3r':
            return _generate_stream_items(data)
        elif informat == 'streamitem':
            ver = self.config['streamcorpus_version']
            if ver not in _message_versions:
                raise ConfigurationError(
                    'Not a valid streamcorpus version: %s '
                    '(choose from: %s)'
                    % (ver, ', '.join(_message_versions.keys())))

            message = _message_versions[ver]
            return streamcorpus.Chunk(data=data, message=message)
        elif informat == 'featurecollection' and FCChunk is not None:
            return FCChunk(data=data)
        else:
            raise ConfigurationError(
                'from_s3_chunks unknown input_format = %r'
                % informat)

    @_retry
    def get_chunk(self, bucket_name, key_path):
        '''return Chunk object full of records
        bucket_name may be None'''
        bucket = get_bucket(self.config, bucket_name=bucket_name)
        key = bucket.get_key(key_path)
        if key is None:
            raise FailedExtraction('Key "%s" does not exist.' % key_path)

        fh = StringIO()
        key.get_contents_to_file(fh)
        data = fh.getvalue()
        if not data:
            raise FailedExtraction('%s: no data (does the key exist?)'
                                   % key.key)

        chunk_type, compression, encryption = parse_file_extensions(key_path)
        if encryption == 'gpg':
            if not self.gpg_decryption_key_path:
                raise FailedExtraction('%s ends with ".gpg" but gpg_decryption_key_path=%s'
                                       % (key.key, self.gpg_decryption_key_path))

        _errors = []
        if compression or encryption:
            _errors, data = decrypt_and_uncompress(
                data, 
                self.gpg_decryption_key_path,
                tmp_dir=self.config.get('tmp_dir_path'),
                compression=compression,
                )
            if not data:
                msg = 'decrypt_and_uncompress got no data for {0!r}, from {1} bytes' \
                    + ' downloaded, errors: {2}' \
                        .format(key_path, len(data), '\n'.join(_errors))
                logger.error(msg)
                raise FailedExtraction(msg)
            logger.info( '\n'.join(_errors) )

        if not self.config['compare_md5_in_file_name']:
            logger.warn('not checking md5 in file name, consider setting '
                        'from_s3_chunks:compare_md5_in_file_name')
        else:
            logger.info('Verifying md5 for "%s"...' % key.key)

            # The regex hammer.
            m = re.search('([a-z0-9]{32})(?:\.|$)', key.key)
            if m is None:
                raise FailedExtraction(
                    'Could not extract md5 from key "%s". '
                    'Perhaps you should disable compare_md5_in_file_name?'
                    % key.key)

            i_content_md5 = m.group(1)
            #import pdb; pdb.set_trace()
            verify_md5(i_content_md5, data, other_errors=_errors)
        return self._decode(data)


class to_s3_chunks(Configured):
    '''Copies chunk files on disk to Amazon S3. The type of data written
    can be specified with the ``output_format`` config option. The
    following values are legal: ``streamitem`` and ``featurecollection``.
    
    N.B. The format is only necessary for construction the
    ``output_name``. The format is also used to pick between an ``fc``
    (for feature collections) extension and a ``sc`` (for stream items)
    extension.

    The following configuration options are mandatory:
    ``bucket`` is the s3 bucket to use. ``aws_access_key_id_path``
    and ``aws_secret_access_key_path`` should point to files
    containing your s3 credentials.

    ``output_name`` is also required and is expanded in the same way as
    the :class:`~streamcorpus_pipeline._local_storage.to_local_chunks`
    writer.  The filename always has ``.{sc,fc}.{xz,gz,sz}`` appended to it
    (depending on the output format specified), and correspondingly,
    the output file is always compressed.  If GPG keys are provided,
    then ``.gpg`` is appended and the file is encrypted.

    The rest of the configuration options are optional and are
    described in the following example:
    
    .. code-block:: yaml

        to_s3_chunks:
          # Mandatory
          bucket: aws-publicdatasets
          aws_access_key_id_path: keys/aws_access_key_id
          aws_secret_access_key_path: keys/aws_secret_access_key
          output_name: "%(date_hour)s/%(source)s-%(num)d-%(input_fname)s-%(md5)s"

          # Optional parameters.

          # The number of times to try writing to s3. A value of
          # `1` means the upload is tried exactly once.
          # The default value is `10`.
          # (This also applies to the verification step.)
          tries: 1

          # When set, the file uploaded will be private.
          # Default: false
          is_private: false

          # When set, the file will be re-downloaded from Amazon, decrypted,
          # decompressed and checksummed against the data sent to Amazon.
          # (This used to be "verify_via_http", but this more broadly named
          # option applies even when "is_private" is true.)
          #
          # Default: true
          verify: true

          # If verification fails `tries` times, then the default
          # behavior is to exit, which can cause a coordinate
          # fork_worker parent to retry the whole job.
          #
          # Default: false
          suppress_failures: false

          # This is prepended to every key given. Namely, all s3 URLs
          # are formed by `s3://{bucket}/{s3_path_prefix}/{input-key-name}`.
          # By default, it is empty.
          s3_path_prefix: some/s3/prefix

          # Paths to GPG keys. Note that if you're using verification,
          # then a decryption key must be given.
          # Default values: None
          gpg_encryption_key_path: keys/gpg-key.pub
          gpg_decryption_key_path: keys/gpg-key.private

          # GPG recipient.
          # Default value: trec-kba
          gpg_recipient: trec-kba

          # Removes the intermediate chunk file from disk.
          # Default: true.
          cleanup_tmp_files: true

          # A temporary directory where intermediate files may reside.
          # Uses your system's default tmp directory (usually `/tmp`)
          # by default.
          tmp_dir_path: /tmp

          # Change compression scheme; default is .xz for greatest
          # compression of archival content (S3 charges by the byte).
          # xz is also the slowest, so other options can make more
          # sense in some applications.  Snappy (.sz) is the fastest
          # and still much better than no compression at all.  Choices
          # are "xz", "sz", "gz", ""
          compression: xz

    '''
    config_name = 'to_s3_chunks'
    default_config = {
        's3_path_prefix': '',
        'tries': 10,
        'gpg_encryption_key_path': None,
        'gpg_decryption_key_path': None,
        'gpg_recipient': 'trec-kba',
        'verify': True,
        'suppress_failures': False,
        'is_private': False,
        'output_format': 'StreamItem',
        'tmp_dir_path': '/tmp',
        'cleanup_tmp_files': True,
        'compression': 'xz',
        # require: bucket, output_name, aws_access_key_id_path,
        #          aws_secret_access_key_path
    }

    def __init__(self, config):
        super(to_s3_chunks, self).__init__(config)

        ## TODO: use something like verify_config
        if self.compression not in known_compression_schemes:
            raise Exception('to_s3_chunks "compression: %r" not in %r' % (
                self.compression, known_compression_schemes))

        logger.critical('compression: %s' % self.config.get('compression'))

        # Backwards compatibility.
        if 'verify_via_http' in self.config:
            logger.warning('Update your config! Use "verify" instead of '
                           '"verify_via_http". The latter is deprecated.')
            self.config['verify'] = self.config.pop('verify_via_http')
            if self.config['verify'] and self.config['is_private']:
                logger.warning('Nonsensical config "verify_via_http=true" and '
                               '"is_private=true". Will verify with boto.')
        self.bucket = get_bucket(self.config)

    def __call__(self, t_path, name_info, i_str):
        '''
        Load chunk from t_path and put it into the right place in s3
        using the output_name template from the config
        '''
        logger.info('to_s3_chunks.call(t_path=%r, name_info=%r, i_str=%r)',
                    t_path, name_info, i_str)
        # Getting name info actually assembles an entire chunk in memory
        # from `t_path`, so we now need to tell it which chunk type to use.
        try:
            more_name_info = get_name_info(t_path, i_str=i_str,
                                           chunk_type=self.chunk_type)
        except Exception, exc:
            logger.critical('failed get_name_info(%r, %r', t_path, i_str, exc_info=True)
            raise

        self.name_info = dict(name_info, **more_name_info)

        if self.name_info['num'] == 0:
            return None

        o_path = self.s3key_name
        logger.info('%s: \n\t%r\n\tfrom: %r\n\tby way of %r',
                    self.__class__.__name__, o_path, i_str, t_path)

        t_path2 = self.prepare_on_disk(t_path)
        data_len = os.path.getsize(t_path2)
        if data_len == 0:
            logger.critical('data is now zero bytes!')
        logger.debug('prepared %s bytes of %r', data_len, t_path2)
        self.put_data(o_path, t_path2, self.name_info['md5'], data_len)

        self.cleanup(t_path, t_path2)
        logger.info('%s finished:\n\t input: %s\n\toutput: %s',
                    self.__class__.__name__, i_str, o_path)
        return [o_path]

    @property
    def outfmt(self):
        return self.config['output_format'].lower()

    @property
    def compression(self):
        return self.config['compression'].lower()

    @property
    def chunk_type(self):
        if self.outfmt == 'featurecollection' and FCChunk is not None:
            if self.config.get('verify'):
                return partial(FCChunk, inline_md5=True)
            return FCChunk
        elif self.outfmt == 'streamitem':
            return streamcorpus.Chunk
        else:
            raise ConfigurationError(
                'Invalid output format: "%s". Choose one of StreamItem '
                'or FeatureCollection.' % self.config['output_format'])

    @property
    def s3key_name(self):
        o_fname = self.config['output_name'] % self.name_info
        ext = 'fc' if self.outfmt == 'featurecollection' else 'sc'
        o_path = os.path.join(self.config['s3_path_prefix'],
                              '%s.%s' % (o_fname, ext))
        if self.compression:
            o_path += '.' + self.compression
        if self.config.get('gpg_encryption_key_path') is not None:
            o_path += '.gpg'
        self.name_info['s3_output_path'] = o_path
        return o_path

    def prepare_on_disk(self, t_path):
        logger.debug('gpg_encryption_key_path: %r', self.config.get('gpg_encryption_key_path'))
        _errors, t_path2 = compress_and_encrypt_path(
            t_path, 
            self.config.get('gpg_encryption_key_path'),
            gpg_recipient=self.config['gpg_recipient'],
            tmp_dir=self.config.get('tmp_dir_path'),
            compression=self.config.get('compression'),
        )
        if len(_errors) > 0:
            logger.error('compress and encrypt errors: %r', _errors)
        return t_path2

    def cleanup(self, *files):
        if not self.config['cleanup_tmp_files']:
            return
        for f in files:
            try:
                os.remove(f)
            except Exception as exc:
                logger.info('%s --> failed to remove %s' % (exc, f))

    @_retry
    def put_data(self, key_path, t_path, md5, data_len=None):
        timedop('s3 put', data_len, lambda: self.put(key_path, t_path))
        if self.config['verify']:
            ok = timedop('s3 verify', data_len, lambda: self.verify(key_path, md5))
            if not ok:
                raise Exception('verify failed putting {0!r}'.format(key_path))

    def put(self, o_path, t_path):
        key = Key(self.bucket, o_path)
        key.set_contents_from_filename(t_path)

        if not self.config.get('is_private', False):
            # Makes the file have a public URL.
            key.set_acl('public-read')

    @_retry
    def verify(self, o_path, md5):
        chunk_format, compression, encryption = parse_file_extensions(o_path)
        logger.info('verifying %s --> %r, %r, %r', o_path, chunk_format, compression, encryption)
        if self.config.get('gpg_encryption_key_path') \
                and not self.config.get('gpg_decryption_key_path'):
            raise ConfigurationError(
                'When "verify=true" and "gpg_encryption_key_path" is set, '
                '"gpg_decryption_key_path" must also be set.')

        if self.config.get('is_private', False):
            rawdata = self.private_data(o_path)
        else:
            rawdata = self.public_data(o_path)

        if not rawdata:
            logger.error('got no data out of reading the data')

        errors, data = decrypt_and_uncompress(
            rawdata,
            self.config.get('gpg_decryption_key_path'),
            tmp_dir=self.config.get('tmp_dir_path'),
            compression=compression,
        )
        if not data:
            logger.error('got no data back from decrypt_and_uncompress %r, (size=%r), errors: %r', o_path, len(rawdata), errors)
            return False

        ### Let's not use both belt and suspenders.  md5 is enough.
        ### Of note, if this gets killed by something, e.g. coordinate
        ### fork_worker parent, then since this is one of the slowest
        ### parts, you might see cbor getting killed, which can look
        ### like this:
        ##    >>> c = list(f)
        ##    ^CTraceback (most recent call last):
        ##    File "<stdin>", line 1, in <module>
        ##    File "/ebs/home/jrf/streamcorpus/py/src/streamcorpus/_chunk.py", line 381, in __iter__
        ##    for msg in self.read_msg_impl():
        ##    File "/ebs/home/jrf/streamcorpus/py/src/streamcorpus/_cbor_chunk.py", line 111, in read_msg_impl
        ##    ob = cbor.load(self._i_chunk_fh)
        ##    RuntimeError: unknown error decoding TAG...
        #
        #try:
        #    count = len(list(self.chunk_type(data=data)))
        #except Exception, exc:
        #    count = None
        #    logger.critical('\n\n********\n\nfailure on %r\n\n********\n\n', o_path, exc_info=True)
        #logger.info('attempting verify of %r %r in %r', count, chunk_format, o_path)
        logger.info('attempting verify of %r in %r', chunk_format, o_path)
        return verify_md5(md5, data, other_errors=errors)

    def public_data(self, o_path):
        url = 'http://s3.amazonaws.com/%(bucket)s/%(o_path)s' % {
            'bucket': self.config['bucket'],
            'o_path': o_path,
        }
        logger.info('public fetching %r', url)
        return requests.get(url).content

    def private_data(self, o_path):
        return self.bucket.get_key(o_path).get_contents_as_string()


class to_s3_tarballs(to_s3_chunks):
    '''
    The same as :class:`streamcorpus_pipeline._s3_storage.to_s3_chunks`,
    except it puts stream items into a gzipped tarball instead of
    chunks. This writer does not do any encryption.

    In addition to the required parameters for ``to_s3_chunks``, this
    writer has three more required parameters: ``tarinfo_name`` (which
    supports ``output_name`` substitution semantics), ``tarinfo_uname``
    and ``tarinfo_gname``.
    '''
    config_name = 'to_s3_tarballs'

    def __init__(self, config):
        super(to_s3_tarballs, self).__init__(config)
        if self.config['output_format'].lower() != 'streamitem':
            raise ConfigurationError(
                'to_s3_tarballs only supports "output_format=streamitem", '
                'but given "output_format=%s"' % self.config['output_format'])

    @property
    def s3key_name(self):
        o_fname = self.config['output_name'] % self.name_info
        return os.path.join(self.config['s3_path_prefix'], o_fname + '.tar.gz')

    def prepare_on_disk(self, t_path):
        t_path2 = tarball_export(self.config, t_path, self.name_info)
        # Cheat a bit here. We are checking the md5 of the full compressed
        # archive instead of the decompressed/decrypted chunk (because this is
        # a tarball, not a chunk).
        # TODO: if md5 is actually needed, read the file and calculate it
        #self.name_info['md5'] = hashlib.md5(data).hexdigest()
        return t_path2

    @_retry
    def redownload_verify(self, o_path, md5):
        key = Key(get_bucket(self.config), o_path)
        data = key.get_contents_as_string()
        logger.info( 'got back SIs: %d' % len( list( Chunk(data=data) ) ))
        return verify_md5(md5, data)

    @_retry
    def verify(self, o_path, md5):
        if self.config.get('is_private', False):
            rawdata = self.private_data(o_path)
        else:
            rawdata = self.public_data(o_path)
        return verify_md5(md5, rawdata)
