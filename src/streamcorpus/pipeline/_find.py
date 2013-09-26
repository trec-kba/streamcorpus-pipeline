'''
KBA pipeline transform that holds a list of known stream_ids and
prints lines to a file of the form:

stream_id,i_str

where i_str is the input task path for the chunk containing the
stream_id.

This software is released under an MIT/X11 open source license.

Copyright 2012 Diffeo, Inc.
'''
import os
import uuid
import random
import logging
logger = logging.getLogger(__name__)

class find(object):
    def __init__(self, config):
        self._config = config
        self._stream_ids = set()
        for line in open(config['list_of_stream_ids_path']).read().splitlines():
            source, stream_id = line.split()
            self._stream_ids.add(stream_id.strip())
        logger.critical('loaded %d stream_ids, e.g. %s' % (len(self._stream_ids), list(self._stream_ids)[0]))
        _path = os.path.join(config['dump_path'], str(uuid.uuid1()) + '.txt')
        _parent_dir = os.path.dirname(_path)
        if not os.path.exists(_parent_dir):
            os.makedirs(_parent_dir)
        self._dump_fh = open(_path, 'wb')
        
        self._special_chunks = set()

    def __call__(self, si, context):

        if si.stream_id in self._stream_ids:

            self._special_chunks.add( context['i_str'] )

            rec = [si.stream_id, context['i_str']]

            line = '\t'.join(rec) + '\n'
            self._dump_fh.write(line)
            self._dump_fh.flush()

            ## pass through all stream_ids that match
            return si

        elif context['i_str'] in self._special_chunks and \
                random.random() < self._config.get('camouflage_fraction'):
            ## for chunks that contain found stream_id, we can pass a
            ## fraction of the remaining StreamItems as camouflage.
            return si

        elif not self._config.get('filter', True):
            ## even it isn't one of the special stream_ids, we might
            ## still let it three if config['filter'] == False
            return si

class find_doc_ids(object):
    def __init__(self, config):
        self._config = config
        self._doc_ids = set()
        for line in open(config['list_of_doc_ids_path']).read().splitlines():
            doc_id = line.split()
            self._doc_ids.add(doc_id.strip())
        logger.critical('loaded %d doc_ids, e.g. %s' % (
                len(self._doc_ids), list(self._doc_ids)[0]))
        _path = os.path.join(config['dump_path'], str(uuid.uuid4()) + '.txt')
        _parent_dir = os.path.dirname(_path)
        if not os.path.exists(_parent_dir):
            os.makedirs(_parent_dir)
        self._dump_fh = open(_path, 'wb')
        
        self._special_chunks = set()

    def __call__(self, si, context):

        assert si.stream_id.split('-')[1] == si.doc_id, \
            (si.stream_id, si.doc_id, context['i_str'])

        if si.doc_id in self._doc_ids:

            self._special_chunks.add( context['i_str'] )

            rec = [si.stream_id, context['i_str']]

            line = '\t'.join(rec) + '\n'
            self._dump_fh.write(line)
            self._dump_fh.flush()

            ## pass through all stream_ids that match
            return si

        elif context['i_str'] in self._special_chunks and \
                random.random() < self._config.get('camouflage_fraction'):
            ## for chunks that contain found stream_id, we can pass a
            ## fraction of the remaining StreamItems as camouflage.
            return si

        elif not self._config.get('filter', True):
            ## even it isn't one of the special stream_ids, we might
            ## still let it three if config['filter'] == False
            return si
