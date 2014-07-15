'''
kba.pipeline incremental Transform for promoting spinn3r's cleansed
content form (called "content_extract") into the clean_html position
if our own attempt to make clean_html from raw failed.

This software is released under an MIT/X11 open source license.

Copyright 2012-2014 Diffeo, Inc.
'''
from __future__ import absolute_import
import sys

from streamcorpus_pipeline._clean_html import force_unicode
from streamcorpus_pipeline._exceptions import InvalidStreamItem
from streamcorpus_pipeline.stages import Configured

class handle_unconvertible_spinn3r(Configured):
    '''
    It seems that some of the spinn3r content is not actually UTF-8
    and there is no record of the original encoding, so we take a shot
    at converting the spinn3r-provided "content_extract" into utf8 and
    using it as the clean_html.  If that fails, we drop the entire
    document from the corpus.
    '''
    config_name = 'handle_unconvertible_spinn3r'

    def __call__(self, si, context):

        if not si.body:
            raise InvalidStreamItem('si.body should never be none: %r' %
                                    si.stream_id)

        if not si.body.raw:
            raise InvalidStreamItem('si.body.raw should never be none: %r' %
                                    si.stream_id)

        if not si.body.clean_html:

            if 'extract' in si.other_content:
                data = si.other_content['extract'].raw
                try:
                    data = data.decode('utf8').encode('utf8')
                except Exception, exc:
                    print('handle_unconvertible_spinn3r: extract.encode("utf8") failed:\n%r' % data)
                    #print data

                    try:
                        data = force_unicode(data)
                    except Exception, exc:
                        print('handle_unconvertible_spinn3r: giving up: %r' % exc)
                        return None

                ## put it on raw, so we clean it up with clean_html
                si.body.raw = data
                si.body.media_type = si.other_content['extract'].media_type
                si.body.encoding = 'UTF-8'

        ## return the si for next stage in pipeline
        return si
