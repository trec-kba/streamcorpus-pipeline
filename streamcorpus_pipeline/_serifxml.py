''':class:`from_serifxml` pipeline reader.

.. This software is released under an MIT/X11 open source license.
   Copyright 2014 Diffeo, Inc.

'''
from __future__ import absolute_import
import logging
import time

from lxml import etree

import streamcorpus
from streamcorpus_pipeline.stages import Configured

logger = logging.getLogger(__name__)

class from_serifxml(Configured):
    '''Read a Serif XML intermediate file as the input to the pipeline.

    This is a specialized reader for unusual circumstances; you will
    still need to run :class:`~streamcorpus_pipeline._serif.serif`
    with special settings to complete the tagging.  This creates a
    bare :attr:`~streamcorpus.StreamItem.tagging` with the input data,
    and fills in the :attr:`~streamcorpus.StreamItem.body`
    :attr:`~streamcorpus.ContentItem.raw` form.

    This has one configuration option, which can usually be left at
    its default value:

    .. code-block:: yaml

        streamcorpus_pipeline:
          from_serifxml:
            tagger_id: serif

    `tagger_id` is the tagger name in the generated
    :class:`~streamcorpus.StreamItem`.

    '''
    config_name = 'from_serifxml'
    default_config = {
        'tagger_id': 'serif',
    }

    def __call__(self, i_str):
        # Read in the entire contents as text; we will need to
        # save it away later
        with open(i_str, 'r') as f:
            serifxml = f.read()

        # Parse the XML
        root = etree.fromstring(serifxml)

        # Get some key parts
        doc_id = root.xpath('string(/SerifXML/Document/@docid)')
        epoch_ticks = time.time() ### NOT IN THE SERIFXML FILE
        stream_time = streamcorpus.make_stream_time(epoch_ticks=epoch_ticks)
        source = root.xpath('string(/SerifXML/Document/@source_type)')
        raw = root.xpath('string(/SerifXML/Document/OriginalText/Contents)')

        # Build the streamitem
        tagging = streamcorpus.Tagging(
            tagger_id=self.config['tagger_id'],
            raw_tagging=serifxml,
        )
        body = streamcorpus.ContentItem(
            raw=raw,
            taggings={
                self.config['tagger_id']: tagging,
            },
        )
        si = streamcorpus.StreamItem(
            version=streamcorpus.Versions.v0_3_0,
            doc_id=doc_id,
            abs_url='',
            source=source,
            body=body,
            stream_id='%d-%s'.format(epoch_ticks, doc_id),
            stream_time=stream_time,
        )
        yield si
