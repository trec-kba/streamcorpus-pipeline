'''
Provides classes for loading chunk files from local storage and
putting them out into local storage.

This software is released under an MIT/X11 open source license.

Copyright 2012 Diffeo, Inc.
'''
import os
import sys
import time
import shutil
import hashlib
import logging
import traceback
import streamcorpus
from _get_name_info import get_name_info

logger = logging.getLogger(__name__)

class from_local_chunks(object):
    def __init__(self, config):
        self.config = config

    def __call__(self, i_str):
        tries = 0
        while tries < self.config['max_retries']:
            try:
                chunk = streamcorpus.Chunk(path=i_str, mode='rb')
                tries += 1
            except IOError:
                ## File is missing?  Assume is slow NFS, keep trying
                time.sleep(2 ** (tries / 6))

        return chunk

class to_local_chunks(object):
    def __init__(self, config):
        self.config = config

    def __call__(self, t_path, name_info, i_str):
        o_type = self.config['output_type']
        
        name_info.update( get_name_info( t_path ) )

        if name_info['num'] == 0:
            return None

        if 'input' in self.config['output_name']:
            i_fname = i_str.split('/')[-1]
            if i_fname.endswith('.sc'):
                i_fname = i_fname[:-3]
            name_info['input_fname'] = i_fname 

        ## prepare to compress the output
        compress = self.config.get('compress', None)
        assert compress in [None, 'xz']

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

        ## if dir is missing make it
        dirname = os.path.dirname(o_path)
        if dirname and not os.path.exists(dirname):
            os.makedirs(dirname)

        if compress:
            assert o_path.endswith('.xz'), o_path
            logger.info('compress_and_encrypt_path(%r)' % t_path)
            errors, t_path2 = streamcorpus.compress_and_encrypt_path(t_path)
            assert not errors
            try:
                os.rename(t_path2, t_path)
                logger.debug('renamed %r --> %r' % (t_path2, t_path))
            except OSError, exc:                
                if exc.errno==18:
                    shutil.copy2(t_path2, t_path)
                    os.remove(t_path2)
                else:
                    logger.critical(traceback.format_exc(exc))
                    raise exc

        ## do an atomic renaming    
        try:
            logger.debug('attemping os.rename(%r, %r)' % (t_path, o_path))
            os.rename(t_path, o_path)
        except OSError, exc:                
            if exc.errno==18:
                shutil.copy2(t_path, o_path)
                os.remove(t_path)
            else:
                msg = 'failed shutil.copy2(%r, %r) and/or os.remove(t_path)\n%s'\
                    % (t_path, o_path, traceback.format_exc(exc))
                logger.critical(traceback.format_exc(exc))
                raise exc
        except Exception, exc:
            msg = 'failed os.rename(%r, %r) -- %s' % (t_path, o_path, traceback.format_exc(exc))
            logger.critical(msg)
            raise exc

        ## return the final output path
        return o_path
