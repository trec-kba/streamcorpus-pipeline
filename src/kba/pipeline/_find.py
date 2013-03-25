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

class find(object):
    def __init__(self, config):
        self._config = config
        self._stream_ids = set(open(config['list_of_stream_ids_path']).read().splitlines())
        _path = os.path.join(config['dump_path'], str(uuid.uuid1()) + '.txt')
        _parent_dir = os.path.dirname(_path)
        if not os.path.exists(_parent_dir):
            os.makedirs(_parent_dir)
        self._dump_fh = open(_path, 'wb')

    def __call__(self, si, context):

        if si.stream_id in self._stream_ids:

            rec = [si.stream_id, context['i_str']]

            line = '\t'.join(rec)
            self._dump_fh.write(line)
            self._dump_fh.flush()

        return si
