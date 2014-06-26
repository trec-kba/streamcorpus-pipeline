'''
An incremental transform that "fixes" text using ftfy from
Luminosolnsight.
'''

from __future__ import absolute_import, division, print_function
from streamcorpus_pipeline.stages import Configured

import ftfy

class fix_text(Configured):
    '''
    Tries to fix poorly encoded Unicode text on a stream item. It expects
    the raw Unicode encoded text in the ``read_from`` field (default is
    ``raw``) and writes the fixed text as UTF8 encoding to the
    ``write_to`` field (default is ``clean_visible``).
    '''
    config_name = 'fix_text'
    default_config = { 'read_from': 'raw', 'write_to': 'clean_visible' }

    def __call__(self, si, context):
        if not si.body or not si.body.raw:
            return si

        read_from = getattr(si.body, self.config['read_from'])
        setattr(si.body, self.config['write_to'],
                ftfy.fix_text(read_from.decode('utf-8')).encode('utf-8'))
        return si
