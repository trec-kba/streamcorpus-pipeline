'''
Provides classes for loading chunk files from local storage and
putting them out into local storage.

This software is released under an MIT/X11 open source license.

Copyright 2012-2014 Diffeo, Inc.
'''
from __future__ import absolute_import
import os
import sys
import time
import errno
import shutil
import hashlib
import logging
import traceback
from cStringIO import StringIO

import streamcorpus
from streamcorpus_pipeline._get_name_info import get_name_info
from streamcorpus_pipeline.stages import Configured
from streamcorpus_pipeline._tarball_export import tarball_export

logger = logging.getLogger(__name__)

_message_versions = {
    'v0_1_0': streamcorpus.StreamItem_v0_1_0,
    'v0_2_0': streamcorpus.StreamItem_v0_2_0,
    'v0_3_0': streamcorpus.StreamItem_v0_3_0,
    }
def _check_version(config, name):
    if config['streamcorpus_version'] not in _message_versions:
        raise yakonfig.ConfigurationError(
            'invalid {} streamcorpus_version {}'
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
    * ``%(doc_ids_8)s``: hyphenated list of 8-character suffixes of document IDs
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
        
        name_info.update( get_name_info( t_path, i_str=i_str ) )

        if name_info['num'] == 0:
            return None

        if 'input' in self.config['output_name']:
            i_fname = i_str.split('/')[-1]
            if i_fname.endswith('.gpg'):
                i_fname = i_fname[:-4]
            if i_fname.endswith('.xz'):
                i_fname = i_fname[:-3]
            if i_fname.endswith('.sc'):
                i_fname = i_fname[:-3]
            name_info['input_fname'] = i_fname 

        ## prepare to compress the output
        compress = self.config.get('compress', None)

        if o_type == 'samedir':
            ## assume that i_str was a local path
            assert i_str[-3:] == '.sc', repr(i_str[-3:])
            o_path = i_str[:-3] + '-%s.sc' % self.config['output_name']
            if compress:
                o_path += '.xz'
            #print 'creating %s' % o_path
            
        elif o_type == 'inplace':
            ## replace the input chunks with the newly created
            o_path = i_str
            if o_path.endswith('.xz'):
                compress = True

        elif o_type == 'otherdir':
            ## put the 
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

        logger.info('writing chunk file to {}'.format(o_path))
        logger.debug('temporary chunk in {}'.format(t_path))

        ## if dir is missing make it
        dirname = os.path.dirname(o_path)
        if dirname and not os.path.exists(dirname):
            os.makedirs(dirname)

        if compress:
            assert o_path.endswith('.xz'), o_path
            logger.info('compress_and_encrypt_path(%r, tmp_dir=%r)', 
                        t_path, self.config['tmp_dir_path'])

            ## forcibly collect dereferenced objects
            #gc.collect()

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
                    if exc.errno==18:
                        logger.debug('resorting to patient_move(%r, %r)',
                                     t_path2, o_path, exc_info=True)
                        patient_move(t_path2, o_path)
                        logger.debug('patient_move succeeded')
                    else:
                        logger.critical('rename failed (%r -> %r)', t_path2, o_path, exc_info=True)
                        raise
                return o_path
            else:
                # for debugging, leave temp file, copy to output
                shutil.copy(t_path2, o_path)
                logger.info('copied %r -> %r', t_path2, o_path)
                return o_path

        if self.config['cleanup_tmp_files']:
            ## do an atomic renaming
            try:
                logger.debug('attemping os.rename(%r, %r)', t_path, o_path)
                os.rename(t_path, o_path)
            except OSError, exc:                
                if exc.errno==18:
                    patient_move(t_path, o_path)
                else:
                    logger.critical(
                        'failed shutil.copy2(%r, %r) and/or os.remove(t_path)',
                        t_path, o_path, exc_info=True)
                    raise
            except Exception, exc:
                logger.critical('failed os.rename(%r, %r)', t_path, o_path, exc_info=True)
                raise
        else:
            # for debugging, leave the tmp file, copy to output position
            shutil.copy(t_path, o_path)
            ogger.info('copied %r -> %r', t_path, o_path)

        ## return the final output path
        return o_path


class to_local_tarballs(Configured):
    config_name = 'to_local_tarballs'

    def __call__(self, t_path, name_info, i_str):
        name_info.update( get_name_info( t_path, i_str=i_str ) )

        if name_info['num'] == 0:
            return None

        o_fname = self.config['output_name'] % name_info
        o_dir = self.config['output_path']
        o_path = os.path.join(o_dir, o_fname + '.tar.gz')

        ## if dir is missing make it
        dirname = os.path.dirname(o_path)
        if dirname and not os.path.exists(dirname):
            os.makedirs(dirname)

        t_path2 = tarball_export(self.config, t_path, name_info)

        ## do an atomic renaming    
        try:
            logger.debug('attemping os.rename(%r, %r)' % (t_path2, o_path))
            os.rename(t_path2, o_path)
        except OSError, exc:                
            if exc.errno==18:
                patient_move(t_path2, o_path)
            else:
                msg = 'failed shutil.copy2(%r, %r) and/or os.remove(t_path)\n%s'\
                    % (t_path2, o_path, traceback.format_exc(exc))
                logger.critical(traceback.format_exc(exc))
                raise
        except Exception, exc:
            msg = 'failed os.rename(%r, %r) -- %s' % (t_path, o_path, traceback.format_exc(exc))
            logger.critical(msg)
            raise

        try:
            os.remove(t_path)
        except Exception, exc:
            logger.critical('failed to os.remove(%r) --> %s' % (t_path, exc))

        logger.info('to_local_tarballs created %s' % o_path)

        ## return the final output path
        return o_path
