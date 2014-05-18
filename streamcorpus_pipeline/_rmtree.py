'''rmtree, can remove directories even when shutil.rmtree fails
because of read-only files in NFS and Windows.

.. This software is released under an MIT/X11 open source license.
   Copyright 2014 Diffeo, Inc.
'''
import os
import sys
import logging
import subprocess
import shutil
import time
import traceback

logger = logging.getLogger(__name__)

def rmtree(path, use_shutil=True, followlinks=False, retries=10):
    '''remove all files and directories below path, including path
    itself; works even when shutil.rmtree fails because of read-only
    files in NFS and Windows.  Follows symlinks.

    `use_shutil` defaults to True; useful for testing

    `followlinks` defaults to False; if set to True, shutil.rmtree is
    not used.
    '''
    if use_shutil and not followlinks:
        try:
            shutil.rmtree(path)
            return
        except Exception, exc:
            logger.info('shutil.rmtree(%s) failed, so resorting to recursive delete', path)
            logger.debug('\ntrapped:\n%s', traceback.format_exc(exc))

    if not os.path.isdir(path):
        os.remove(path)
        return

    ## bottom up traversal removing files and then removing directories
    for root, dir_names, file_names in os.walk(path, topdown=False, followlinks=followlinks):
        for fname in file_names:
            fpath = os.path.join(root, fname)
            tries = 0
            while tries < retries:
                tries += 1
                try:
                    os.remove(fpath)
                    break
                except Exception, exc:
                    time.sleep(0.1)
            if os.path.exists(fpath):
                logger.critical('os.remove(%s) failed, so leaving data behind!!!', fpath)
                logger.critical('\ntrapped:\n%s', traceback.format_exc(exc))
                #logger.critical(get_open_fds())
        for dname in dir_names:
            full_path = os.path.join(root, dname)
            if os.path.islink(full_path):
                real_path = os.path.realpath(full_path)
                os.remove(full_path)
                full_path = real_path
            os.rmdir(full_path)

    if os.path.exists(path):
        os.rmdir(path)

def get_open_fds(verbose=False):
    '''return list of open files for current process

    .. warning: will only work on UNIX-like os-es.
    '''
    pid = os.getpid()
    procs = subprocess.check_output( 
        [ "lsof", '-w', '-Ff', "-p", str( pid ) ] )
    if verbose:
        oprocs = subprocess.check_output( 
            [ "lsof", '-w', "-p", str( pid ) ] )
        logger.info(oprocs)
    open_files = filter( 
        lambda s: s and s[ 0 ] == 'f' and s[1: ].isdigit(),
        procs.split( '\n' ) )
    return open_files
