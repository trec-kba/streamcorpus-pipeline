'''
Transforms the JSON format used in the original KBA corpus into the
current streamcorpus format

This software is released under an MIT/X11 open source license.

Copyright 2012 Diffeo, Inc.
'''

## this assumes that streamcorpus has been installed
from streamcorpus import make_stream_item, ContentItem

import os
import json
import gzip

def convert_kba_json(config):
    '''
    Returns a kba.pipeline "extractor" that generates a single
    streamcorpus.Chunk file containing the John Smith corpus.
    '''
    def _convert_kba_json(path_to_original):
        yield generate_stream_items_from_kba_json(path_to_original)
    
    return _convert_kba_json

def make_content_item(ci):
    '''
    converts an in-memory content item generated from deserializing a
    json content-item into a thrift ContentItem
    '''
    ## return empty content item if received None
    if ci is None:
        return ContentItem()

    ## we expect everything UTF-8 already from this source
    assert ci['encoding'] == 'UTF-8', ci['encoding']

    ## convert the cleansed and ner to byte arrays, or None
    #cleansed = ci.pop('cleansed', b'').decode('string-escape')
    #ner      = ci.pop('ner',      b'').decode('string-escape')
    ## construct a ContentItem and return it
    return ContentItem(
        clean_html = ci.pop('raw').decode('string-escape'),
        encoding = 'UTF-8',
        ## Let's not include these forms of the data; We will
        ## regenerate the cleansed in a byte-offset preserving manner.
        #cleansed = cleansed,
        #ner = ner
        )

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
        stream_item.body = make_content_item(doc.pop('body'))

        title = doc.pop('title', None)
        if title:
            title = make_content_item( title )
            ## put the title into body.clean_html
            stream_item.body.clean_html = '<p>' + title.clean_html + \
                ' </p>\n ' + stream_item.body.clean_html
            stream_item.other_content['title']  = title

        anchor = doc.pop('anchor', None)
        if anchor:
            anchor = make_content_item( anchor )
            ## put the anchor into body.clean_html
            stream_item.body.clean_html = '<p>' + anchor.clean_html + \
                ' </p>\n ' + stream_item.body.clean_html
            stream_item.other_content['anchor'] = anchor

        yield stream_item
