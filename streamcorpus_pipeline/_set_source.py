'''Incremental transforms that sets the `StreamItem.source` to a
configured value.

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2015 Diffeo, Inc.

'''
from __future__ import absolute_import
import logging

from streamcorpus_pipeline.stages import Configured


logger = logging.getLogger(__name__)


class set_source(Configured):
    config_name = 'set_source'
    default_config = {
        'new_source': None
    }
    def __call__(self, si, context=None):
        si.source = self.config.get('new_source')
        return si
