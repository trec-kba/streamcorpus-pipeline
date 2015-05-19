'''transform that adds a nilsimsa hash to `other_content`

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2015 Diffeo, Inc.

'''
from __future__ import absolute_import

from nilsimsa import Nilsimsa

from streamcorpus_pipeline.stages import Configured
from streamcorpus_pipeline._clean_visible import cleanse
from streamcorpus import ContentItem


class nilsimsa(Configured):
    '''Creates :attr:`~streamcorpus.StreamItem.other_content` with key `nilsimsa`
    and a `raw` property set to
    `Nilsimsa(cleanse(si.body.clean_visible)).hexdigest()`

    This has no configuration options.

    '''
    config_name = 'nilsimsa'
    def __call__(self, si, context):
        
        if si.body and si.body.clean_visible:
            text = cleanse(si.body.clean_visible.decode('utf8'))
            nhash = Nilsimsa(text.encode('utf8')).hexdigest()
            ci = ContentItem(raw=nhash)
            si.other_content['nilsimsa'] = ci

        return si
