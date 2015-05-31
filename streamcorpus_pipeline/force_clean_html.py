'''Do our best to fill in body.clean_html.

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2015 Diffeo, Inc.

.. autoclass:: force_clean_html
'''
from __future__ import absolute_import, division, print_function
import logging

from streamcorpus_pipeline._clean_html import make_clean_html
from streamcorpus_pipeline._clean_visible import clean_visible
from streamcorpus_pipeline._exceptions import InvalidStreamItem
from streamcorpus_pipeline.stages import Configured


logger = logging.getLogger(__name__)


class force_clean_html(Configured):
    '''force :attr:`~StreamItem.body.clean_html` to be populated or
    rejects the StreamItem.

    '''
    config_name = 'force_clean_html'
    def __call__(self, stream_item, context):
        if stream_item.body.clean_html is not None and \
           len(stream_item.body.clean_html) > 0:
            return stream_item
        if stream_item.body.clean_visible is None:
            logger.warning('stream item %s has neither clean_visible nor '
                           'clean_html', stream_item.stream_id)
            raise InvalidStreamItem

        # With only clean visible, the best we can do is wrap it in a <pre>
        # tag and hope for the best.
        clean_vis_as_html = '<pre>%s</pre>' % stream_item.body.clean_visible
        stream_item.body.clean_html = make_clean_html(
            clean_vis_as_html, stream_item=stream_item)
        # Since `clean_visible` has several invariants coupled with
        # `clean_html`, we need to regenerate it. It's a bit circuitous, but
        # less likely to fail.
        stream_item = clean_visible({})(stream_item, context)

        # check again to make sure we got something
        if stream_item.body.clean_html is not None and \
           len(stream_item.body.clean_html) > 0:
            return stream_item
