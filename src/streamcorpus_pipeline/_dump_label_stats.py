'''
KBA pipeline transform for dumping statistics from labels

This software is released under an MIT/X11 open source license.

Copyright 2012-2013 Diffeo, Inc.
'''
import os
import uuid
from streamcorpus import OffsetType

class dump_label_stats(object):
    def __init__(self, config):
        self.config = config
        self._path = os.path.join(self.config['dump_path'], str(uuid.uuid1()) + '.txt')
        self._dump_fh = open(self._path, 'wb')

    def __call__(self, si, context):
        if self.config['annotator_id'] not in si.body.labels:
            return si
        for label in si.body.labels[self.config['annotator_id']]:
            begin = label.offsets[OffsetType.BYTES].first
            end   = label.offsets[OffsetType.BYTES].length + begin
            text = si.body.clean_visible[begin:end]

            ## make a record with stream_id
            rec = [si.stream_id, label.target.target_id, text]

            line = '\t'.join(rec)
            self._dump_fh.write(line)
        self._dump_fh.flush()
        return si
