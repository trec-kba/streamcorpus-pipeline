'''
Provides classes for loading chunk files from local storage and
putting them out into local storage.

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2014 Diffeo, Inc.

'''
from __future__ import absolute_import
import errno
import logging
import os
import shutil
import time

import streamcorpus
from streamcorpus_pipeline._get_name_info import get_name_info
from streamcorpus_pipeline.stages import Configured
from streamcorpus_pipeline._tarball_export import tarball_export
from yakonfig import ConfigurationError

logger = logging.getLogger(__name__)

_message_versions = {
    'v0_1_0': streamcorpus.StreamItem_v0_1_0,
    'v0_2_0': streamcorpus.StreamItem_v0_2_0,
    'v0_3_0': streamcorpus.StreamItem_v0_3_0,
    }


def _check_version(config, name):
    if config['streamcorpus_version'] not in _message_versions:
        raise ConfigurationError(
            'invalid {0} streamcorpus_version {1}'
            .format(name, config['streamcorpus_version']))


class from_local_chunks(Configured):
    '''
    may use config['max_retries'] (default 1)
    may use config['max_backoff'] (seconds, default 300)
    may use config['streamcorpus_version'] (default 'v0_3_0')
    '''
    config_name = 'from_local_chunks'
    default_config = {
        'max_backoff': 300,
        'streamcorpus_version': 'v0_3_0',
    }

    @staticmethod
    def check_config(config, name):
        _check_version(config, name)

    def __call__(self, i_str):
        backoff = 0.1
        start_time = time.time()
        tries = 0
        max_retries = int(self.config.get('max_retries', 1))
        last_exc = None
        while tries < max_retries:
            try:
                message = _message_versions[self.config['streamcorpus_version']]
                logger.debug('reading from %r' % i_str)
                chunk = streamcorpus.Chunk(path=i_str, mode='rb', message=message)
                return chunk
            except IOError, exc:
                if exc.errno == errno.ENOENT:
                    logger.critical('File is missing?  Assume is slow NFS, try %d more times',
                                    max_retries - tries)
                    backoff *= 2
                    tries += 1
                    elapsed = time.time() - start_time
                    if elapsed > self.config['max_backoff']:
                        ## give up after five minutes of retries
                        logger.critical('File %r not found after %d retries', i_str, tries)
                        raise
                    time.sleep(backoff)
                    last_exc = exc
                else:
                    logger.critical('failed loading %r', i_str, exc_info=True)
                    raise

        ## exceeded max_retries
        logger.critical('File %r not found after %d retries', i_str, tries)
        if last_exc:
            raise last_exc
        raise IOError('File %r not found after %d retries' % (i_str, tries))


def patient_move(path1, path2, max_tries=30):
    backoff = 0.1
    tries = 0
    while tries < max_tries:
        try:
            shutil.copy2(path1, path2)
        except IOError, exc:
            if exc.errno == 2:
                tries += 1
                if tries > max_tries:
                    logger.critical('%d tries failed copying %r to %r', tries, path1, path2, exc_info=True)
                    raise
                logger.critical('attempting retry on shutil.copy2(%r, %r)', path1, path2)
                backoff *= 2
                time.sleep(backoff)
                continue
            else:
                logger.critical('failed copy %r -> %r', path1, path2, exc_info=True)
                raise

        try:
            os.remove(path1)
        except Exception, exc:
            logger.critical('ignoring failure to os.remove(%r)', path1)
            pass

        ## if we get here, we succeeded
        return

    # should never get here
    raise Exception('weird error inside patient_move(%r, %r)' % (path1, path2))


class from_local_files(Configured):
    '''Read plain text or other raw content from local files.

    Each input filename is taken as an actual filename.  This produces
    one stream item per file.  This can be configured as follows:

    .. code-block:: yaml

        abs_url: null
        url_prefix: file://
        absolute_filename: true
        epoch_ticks: file
        encoding: null

    If `abs_url` is provided in configuration, this value is used
    as-is as the corresponding field in the stream item.  Otherwise,
    the path is converted to an absolute path if `absolute_filename`
    is true, and then `url_prefix` is prepended to it.

    `epoch_ticks` indicates the corresponding time stamp in the stream
    item.  This may be an integer for specific seconds since the Unix
    epoch, the string ``file`` indicating to use the last-modified
    time of the file, or the string ``now`` for the current time.

    ``encoding`` should be set if you know the encoding of the files
    you're loading. e.g., ``utf-8``.

    The values shown above are the defaults.  If reading in files
    in a directory structure matching a URL hierarchy, an alternate
    configuration could be

    .. code-block:: yaml

        url_prefix: "http://home.example.com/"
        absolute_path: false

    '''
    config_name = 'from_local_files'
    default_config = {
        'abs_url': None,
        'url_prefix': 'file://',
        'absolute_filename': True,
        'epoch_ticks': 'file',
        'encoding': None,
    }

    def __init__(self, config):
        super(from_local_files, self).__init__(config)
        self.abs_url = self.config.get('abs_url', None)
        self.url_prefix = self.config.get('url_prefix', '')
        self.absolute_filename = self.config.get('absolute_filename', False)
        self.epoch_ticks = self.config.get('epoch_ticks', 'file')
        self.encoding = self.config.get('encoding')

        if ((not isinstance(self.epoch_ticks, int) and
             self.epoch_ticks not in ['file', 'now'])):
            self.epoch_ticks = 'file'

    def __call__(self, i_str):
        if self.epoch_ticks == 'file':
            st = os.stat(i_str)
            epoch_ticks = st.st_mtime
        elif self.epoch_ticks == 'now':
            epoch_ticks = time.time()
        else:
            epoch_ticks = self.epoch_ticks

        if self.abs_url:
            abs_url = self.abs_url
        else:
            abs_url = i_str
            if self.absolute_filename:
                abs_url = os.path.abspath(abs_url)
            if self.url_prefix:
                abs_url = self.url_prefix + abs_url

        si = streamcorpus.make_stream_item(epoch_ticks, abs_url)
        with open(i_str, 'rb') as f:
            si.body.raw = f.read()
        if self.encoding is not None:
            si.body.encoding = self.encoding

        yield si


class to_local_chunks(Configured):

    '''Write output to a local chunk file.

    .. warning:: This stage must be listed last in the ``writers`` list.
                 For efficiency it renames the intermediate chunk file,
                 and so any subsequent writer stages will have no input
                 chunk file to work with.

    This stage may take several additional configuration parameters.

    .. code-block:: yaml

        output_type: samedir

    Where to place the output: ``inplace`` replaces the input file;
    ``samedir`` uses the `output_name` in the same directory as the
    input; and ``otherdir`` uses `output_name` in `output_path`.

    .. code-block:: yaml

        output_path: /home/diffeo/outputs

    If `output_type` is ``otherdir``, the name of the directory to
    write to.

    .. code-block:: yaml

        output_name: output-%(first)d.sc

    Gives the name of the output file if `output_type` is not
    ``inplace``.  If this is ``input`` then the output file name is
    automatically derived from the input file name.  This may also
    include Python format string parameters:

    * ``%(input_fname)s``: the basename of the input file name
    * ``%(input_md5)s``: the last part of a hyphenated input filename
    * ``%(md5)s``: MD5 hash of the chunk file
    * ``%(first)d``: index of the first item in the chunk file
    * ``%(num)d``: number of items in the chunk file
    * ``%(epoch_ticks)d``: timestamp of the first item in the chunk file
    * ``%(target_names)s``: hyphenated list of rating target names
    * ``%(source)s``: source name for items in the chunk file
    * ``%(doc_ids_8)s``: hyphenated list of 8-character suffixes of
      document IDs
    * ``%(date_hour)s``: partial timestamp including date and hour
    * ``%(rand8)s``: 8-byte random hex string

    .. code-block:: yaml

        compress: true

    If specified, append ``.xz`` to the output file name and
    LZMA-compress the output file.  Defaults to false.

    .. code-block:: yaml

        cleanup_tmp_files: false

    If set to "false", do not delete the intermediate file (or rename
    it away), enabling later writer stages to be run.  Defaults to
    true.

    '''
    config_name = 'to_local_chunks'
    default_config = {
        'cleanup_tmp_files': True,
        'compress': False,
    }

    def __call__(self, t_path, name_info, i_str):
        o_type = self.config['output_type']

        name_info.update(get_name_info(t_path, i_str=i_str))

        if name_info['num'] == 0:
            return None

        output_name = self.config.get('output_name')
        if output_name and ('input' in output_name):
            i_fname = i_str.split('/')[-1]
            if i_fname.endswith('.gpg'):
                i_fname = i_fname[:-4]
            if i_fname.endswith('.xz'):
                i_fname = i_fname[:-3]
            if i_fname.endswith('.sc'):
                i_fname = i_fname[:-3]
            name_info['input_fname'] = i_fname

        # prepare to compress the output
        compress = self.config.get('compress', None)

        if o_type == 'samedir':
            # assume that i_str was a local path
            assert i_str[-3:] == '.sc', repr(i_str[-3:])
            o_path = i_str[:-3] + '-%s.sc' % self.config['output_name']
            if compress:
                o_path += '.xz'
            # print 'creating %s' % o_path

        elif o_type == 'inplace':
            # replace the input chunks with the newly created
            o_path = i_str
            if o_path.endswith('.xz'):
                compress = True

        elif o_type == 'otherdir':
            if not self.config['output_path'].startswith('/'):
                o_dir = os.path.join(os.getcwd(), self.config['output_path'])
            else:
                o_dir = self.config['output_path']

            if not os.path.exists(o_dir):
                os.makedirs(o_dir)

            o_fname = self.config['output_name'] % name_info
            o_path = os.path.join(o_dir, o_fname + '.sc')
            if compress:
                o_path += '.xz'
        elif o_type == 'pathfmt':
            # uses output_name, but in a totally different way, to
            # make 'input' checking above work.
            fmt = self.config['output_name']
            o_path = fmt % name_info
            o_dir = os.path.dirname(o_path)
            if not os.path.exists(o_dir):
                os.makedirs(o_dir)

        logger.info('writing chunk file to {0}'.format(o_path))
        logger.debug('temporary chunk in {0}'.format(t_path))

        # if dir is missing make it
        dirname = os.path.dirname(o_path)
        if dirname and not os.path.exists(dirname):
            os.makedirs(dirname)

        if compress:
            assert o_path.endswith('.xz'), o_path
            logger.info('compress_and_encrypt_path(%r, tmp_dir=%r)',
                        t_path, self.config['tmp_dir_path'])

            errors, t_path2 = streamcorpus.compress_and_encrypt_path(
                t_path, tmp_dir=self.config['tmp_dir_path'])
            assert not errors, errors

            if self.config['cleanup_tmp_files']:
                # default action, move tmp file to output position
                try:
                    logger.debug('attempting renamed(%r, %r)', t_path2, o_path)
                    os.rename(t_path2, o_path)
                    logger.debug('renamed(%r, %r)', t_path2, o_path)
                except OSError, exc:
                    if exc.errno == errno.EXDEV:
                        logger.debug('resorting to patient_move(%r, %r)',
                                     t_path2, o_path, exc_info=True)
                        patient_move(t_path2, o_path)
                        logger.debug('patient_move succeeded')
                    else:
                        logger.error('rename failed (%r -> %r)',
                                     t_path2, o_path, exc_info=True)
                        raise
                return [o_path]
            else:
                # for debugging, leave temp file, copy to output
                shutil.copy(t_path2, o_path)
                logger.info('copied %r -> %r', t_path2, o_path)
                return [o_path]

        if self.config['cleanup_tmp_files']:
            # do an atomic renaming
            try:
                logger.debug('attemping os.rename(%r, %r)', t_path, o_path)
                os.rename(t_path, o_path)
            except OSError, exc:
                if exc.errno == errno.EXDEV:
                    patient_move(t_path, o_path)
                else:
                    logger.error(
                        'failed shutil.copy2(%r, %r) and/or '
                        'os.remove(t_path)',
                        t_path, o_path, exc_info=True)
                    raise
            except Exception, exc:
                logger.error('failed os.rename(%r, %r)', t_path, o_path,
                             exc_info=True)
                raise
        else:
            # for debugging, leave the tmp file, copy to output position
            shutil.copy(t_path, o_path)
            logger.info('copied %r -> %r', t_path, o_path)

        # return the final output path
        return [o_path]


class to_local_tarballs(Configured):
    config_name = 'to_local_tarballs'

    def __call__(self, t_path, name_info, i_str):
        name_info.update(get_name_info(t_path, i_str=i_str))

        if name_info['num'] == 0:
            return None

        o_fname = self.config['output_name'] % name_info
        o_dir = self.config['output_path']
        o_path = os.path.join(o_dir, o_fname + '.tar.gz')

        # if dir is missing make it
        dirname = os.path.dirname(o_path)
        if dirname and not os.path.exists(dirname):
            os.makedirs(dirname)

        t_path2 = tarball_export(self.config, t_path, name_info)

        # do an atomic renaming
        try:
            logger.debug('attemping os.rename(%r, %r)' % (t_path2, o_path))
            os.rename(t_path2, o_path)
        except OSError, exc:
            if exc.errno == errno.EXDEV:
                patient_move(t_path2, o_path)
            else:
                logger.error('failed shutil.copy2(%r, %r) and/or '
                             'os.remove(t_path)',
                             t_path2, o_path, exc_info=True)
                raise
        except Exception, exc:
            logger.error('failed os.rename(%r, %r)',
                         t_path, o_path, exc_info=True)
            raise

        try:
            os.remove(t_path)
        except Exception, exc:
            logger.critical('failed to os.remove(%r) --> %s' % (t_path, exc))

        logger.info('to_local_tarballs created %s' % o_path)

        # return the final output path
        return [o_path]
