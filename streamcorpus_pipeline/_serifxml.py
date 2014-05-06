''':class:`from_serifxml` pipeline reader.

.. This software is released under an MIT/X11 open source license.
   Copyright 2014 Diffeo, Inc.

'''
from __future__ import absolute_import
import logging
import os
import re
import time

from lxml import etree

import streamcorpus
from streamcorpus_pipeline.stages import Configured

logger = logging.getLogger(__name__)

date_in_file_name_re = re.compile('^(?P<year>(19|20)\d{2})(?P<month>\d{2})(?P<day>\d{2})(-|_|.)')

class from_serifxml(Configured):
    '''Read a Serif XML intermediate file as the input to the pipeline.

    This is a specialized reader for unusual circumstances; you will
    still need to run :class:`~streamcorpus_pipeline._serif.serif`
    with special settings to complete the tagging.  This expects to
    find serifxml flat files in a directory and creates a
    :class:`~streamcorpus.Tagging` with
    :attr:`~streamcorpus.Tagging.raw_tagging` holding the serifxml
    string.  This :class:`~streamcorpus.Tagging` is stored in
    :attr:`~streamcorpus.StreamItem.body.taggings`.

    This also fills in :attr:`~streamcorpus.ContentItem.raw` field.

    This has one configuration option, which can usually be left at
    its default value:

    .. code-block:: yaml

        streamcorpus_pipeline:
          from_serifxml:
            tagger_id: serif

    `tagger_id` is the tagger name in the generated
    :class:`~streamcorpus.StreamItem`.

    To obtain :attr:`~streamcorpus.StreamItem.body.sentences`, one
    must run Serif in the special `read_serifxml` mode:

    .. code-block:: yaml

        streamcorpus_pipeline:
          third_dir_path: /third
          tmp_dir_path: tmp
          reader: from_serifxml
          incremental_transforms:
          - language
          - guess_media_type
          - clean_html
          - clean_visible
          - title
          batch_transforms:
          - serif
          language:
            force:
              name: English
              code: en
          guess_media_type:
            fallback_media_type: text/plain
          serif:
            path_in_third: serif/serif-latest
            serif_exe: bin/x86_64/Serif
            par: streamcorpus_read_serifxml
            par_additions:
              streamcorpus_read_serifxml:
              - "# example additional line"
          writer: to_local_chunks
          to_local_chunks:
            output_type: otherdir
            output_path: test_output
            output_name: "%(input_fname)s"

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

        fname = os.path.basename(i_str)
        stream_time = None
        date_m = date_in_file_name_re.match(fname)
        if date_m:
            year = int(date_m.group('year'))
            month = int(date_m.group('month'))
            day = int(date_m.group('day'))
            try:
                stream_time = streamcorpus.make_stream_time(
                    zulu_timestamp = '%d-%02d-%02dT00:00:01.000000Z' % (year, month, day))
            except Exception, exc:
                logger.info('trapped failed parsing of file name to make stream_time',
                            exc_info=True)
                stream_time = None

        if not stream_time:
            ## fall back to using the present moment on this system
            epoch_ticks = time.time() ### NOT IN THE SERIFXML FILE
            stream_time = streamcorpus.make_stream_time(epoch_ticks=epoch_ticks)

        # Parse the XML
        root = etree.fromstring(serifxml)

        # Get some key parts
        doc_id = root.xpath('string(/SerifXML/Document/@docid)')
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
            abs_url=fname,
            source=source,
            body=body,
            stream_id='%d-%s' % (stream_time.epoch_ticks, doc_id),
            stream_time=stream_time,
        )
        yield si
