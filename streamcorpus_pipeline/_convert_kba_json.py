'''
Transforms the JSON format used in the original KBA corpus into the
current streamcorpus format

This software is released under an MIT/X11 open source license.

Copyright 2012-2013 Diffeo, Inc.
'''
from __future__ import absolute_import
import traceback
import gzip
import os

import json

from streamcorpus import make_stream_item, ContentItem
from streamcorpus_pipeline.stages import Configured

class convert_kba_json(Configured):
    '''
    Returns a streamcorpus_pipeline "reader" that generates a single
    streamcorpus.Chunk file containing the John Smith corpus.
    '''
    config_name = 'convert_kba_json'
    def __call__(self, path_to_original):
        return generate_stream_items_from_kba_json(path_to_original)

def generate_stream_items_from_kba_json(json_file_path):
    ## iterate over gzip'ed file of JSON lines
    data = gzip.GzipFile(fileobj=open(json_file_path, 'rb'), mode='rb').read()
    for line in data.splitlines():
        try: 
            doc = json.loads(line)
        except Exception, exc: 
            print('trapped: %s' % traceback.format_exc(exc))
            print('continuing')
            continue

        assert doc['source'] == 'social', doc['source']

        ## make a StreamItem with valid StreamTime computed from
        ## zulu_timestamp.  This will fix the four-hour offsets in
        ## some of the KBA 2012 files.
        stream_item = make_stream_item(
            doc['stream_time']['zulu_timestamp'],
            bytes(doc['abs_url'].encode('utf-8'))
            )

        ## capture schost and source
        stream_item.schost = doc.pop('schost')
        stream_item.source = doc.pop('source')

        ## assemble source_metadata
        stream_item.source_metadata['kba-2012'] = json.dumps(doc.pop('source_metadata'))
        
        ## might have a funky original URL
        stream_item.original_url = doc['original_url'] and \
            bytes(doc['original_url'].encode('utf-8')) or b''

        ## get the three possible ContentItems
        body   = doc.pop('body',   {}).pop('raw', '').decode('string-escape')
        title  = doc.pop('title',  {}).pop('raw', '').decode('string-escape')
        anchor = doc.pop('anchor', {}).pop('raw', '').decode('string-escape')

        stream_item.body = ContentItem(
            raw = b''.join(['<p>', anchor, '</p>',
                            '<p>', title, '</p>',
                            body]),
            media_type = 'text/html',
            encoding = 'UTF-8',
            )

        if title:
            stream_item.other_content['title']  = ContentItem(
                raw = title,
                media_type = 'text/html',
                encoding = 'UTF-8',
                )

        if anchor:
            stream_item.other_content['anchor']  = ContentItem(
                raw = anchor,
                media_type = 'text/html',
                encoding = 'UTF-8',
                )

        yield stream_item
