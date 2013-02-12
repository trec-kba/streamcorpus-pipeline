'''
Provides classes for loading chunk files from local storage and
putting them out into local storage.

This software is released under an MIT/X11 open source license.

Copyright 2012 Diffeo, Inc.
'''
import os
import time
import hashlib
import streamcorpus

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
        
        if o_type == 'samedir':
            ## assume that i_str was a local path
            assert i_str[-3:] == '.sc', repr(i_str[-3:])
            o_path = i_str[:-3] + '-%s.sc' % self.config['output_name']
            #print 'creating %s' % o_path
            
        elif o_type == 'inplace':
            ## replace the input chunks with the newly created
            o_path = i_str

        elif o_type == 'otherdir':
            ## put the 
            if not self.config['output_path'].startswith('/'):
                o_dir = os.path.join(os.getcwd(), self.config['output_path'])
            else:
                o_dir = self.config['output_path']

            if not os.path.exists(o_dir):
                os.makedirs(o_dir)

            ## must compute md5 if needed by the output_name
            if 'md5' in self.config['output_name']:
                _md5 = hashlib.md5()
                map(_md5.update, open(t_path))
                name_info['md5'] = _md5.hexdigest()

            o_fname = self.config['output_name'] % name_info
            o_path = os.path.join(o_dir, o_fname + '.sc')

        ## do an atomic renaming    
        os.rename(t_path, o_path)

        ## return the final output path
        return o_path
