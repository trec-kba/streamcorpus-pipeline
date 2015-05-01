from __future__ import absolute_import

import boto
from boto.s3.key import Key
import json
import os

from streamcorpus import Chunk
from streamcorpus_pipeline._s3_storage import _retry, \
    verify_md5, to_s3_chunks
from streamcorpus_pipeline._gather_frequencies import \
    gather_frequencies

class to_s3_frequencies(to_s3_chunks):

    config_name = 'to_s3_frequencies'

    def __init__(self, config):
        super(to_s3_frequencies, self).__init__(config)

    def prepare_on_disk(self, t_path):
        # Do document counting.
        xform_config = {'tagger_id': self.config['tagger_id']}
        freq_xform = gather_frequencies(xform_config)
        chunk = Chunk(path=t_path)
        context = {}
        for si in chunk:
            si = freq_xform(si, context)
        json_path = t_path + '.json'
        json.dump(context['string_counts'], open(json_path, 'w'))
        return json_path

    @property
    def s3key_name(self):
        o_fname = self.config['output_name'] % self.name_info
        return os.path.join(self.config['s3_path_prefix'], o_fname + '.json')

    @_retry
    def redownload_verify(self, o_path, md5):
        key = Key(get_bucket(self.config), o_path)
        data = key.get_contents_as_string()
        return True

    @_retry
    def verify(self, o_path, md5):
        return True
