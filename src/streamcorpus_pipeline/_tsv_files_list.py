'''
Transforms a corpus into the streamcorpus format.

Data is loaded from a file specifying the documents in the corpus. The
file should have a yaml metadata "header" and a tab-separated-values
list of documents in the body. The header should be delimited by lines
containing "---" by itself. Example:

---
abs_url_base: <abs_url_base>
source: <source>
annotator_id: <annotator_id>
---
<target_id>	<mention>	<document_path>
<target_id>	<mention>	<document_path>

Copyright 2012-2013 Diffeo, Inc.
'''

## this assumes that streamcorpus has been installed
import streamcorpus
from streamcorpus_pipeline._exceptions import ConfigurationError

import logging

import os
import re
import logging
import csv
import yaml
import magic
from streamcorpus_pipeline._clean_visible import cleanse

logger = logging.getLogger(__name__)

class tsv_files_list(object):
    def __init__(self, config):
        self.config = config

    def __call__(self, path_to_input_file):
        '''
        This _looks_ like a Chunk only in that it generates StreamItem
        instances when iterated upon.
        '''
        path_to_input_file = os.path.abspath(path_to_input_file)

        with open(path_to_input_file) as f:
            ## read yaml metadata
            lines = []
            for line in f:
                if line == '---\n' and lines != []:
                    break
                lines.append(line)
            metadata = yaml.load(''.join(lines))

            ## read tsv entries
            entries = []
            for row in csv.reader(f, delimiter='\t'):
                entries.append(row)

        base_dir = os.path.dirname(path_to_input_file)

        if 'abs_url_base' not in metadata:
            metadata['abs_url_base'] = path_to_input_file
            logger.warn('abs_url_base not set, defaulting to %s' % path_to_input_file)

        if 'source' not in metadata:
            metadata['source'] = 'unknown'
            logger.warn('source not set, defaulting to unknown')

        if 'annotator_id' not in metadata:
            metadata['annotator_id'] = 'unknown'
            logger.warn('annotator_id not set, defaulting to unknown')

        for target_id, mention, path in entries:
            ## normalize path
            if not os.path.isabs(path):
                path = os.path.join(base_dir, path)

            ## recurse through directories
            if os.path.isdir(path):
                for dirpath, dirnames, filenames in os.walk(path):
                    for fname in filenames:
                        fpath = os.path.join(base_dir, dirpath, fname)
                        yield self._make_stream_item(fpath, target_id, mention, metadata)
            else:
                yield self._make_stream_item(path, target_id, mention, metadata)

    def _ensure_unique(self, sym):
        ## Generate a unique identifier by appending a numeric suffix
        ## Used to generate unique urls for stream items
        if not hasattr(self, '_sym_cache'):
            self._sym_cache = set()
        if sym in self._sym_cache:
            i = 1
            while True:
                newsym = sym + '-' + str(i)
                if newsym not in self._sym_cache:
                    sym = newsym
                    break
                i += 1
        self._sym_cache.add(sym)
        return sym

    def _make_stream_item(self, path, target_id, mention, metadata):

        ## Every StreamItem has a stream_time property.  It usually comes
        ## from the document creation time.  Here, we assume the JS corpus
        ## was created at one moment at the end of 1998:
        creation_time = '1998-12-31T23:59:59.999999Z'

        ## construct abs_url
        abs_url = os.path.join(
            metadata['abs_url_base'],
            target_id,
            path.split(os.path.sep)[-1])

        ## make stream item
        stream_item = streamcorpus.make_stream_item(
            creation_time,
            self._ensure_unique(abs_url))

        ## build a ContentItem for the body
        body = streamcorpus.ContentItem()
        body.media_type = magic.from_file(path, mime=True)
        with open(path) as f:
            body.raw = f.read()

        ## attach the content_item to the stream_item
        stream_item.body = body

        ## annotations
        anno = streamcorpus.Annotator()
        anno.annotator_id = metadata['annotator_id']
        anno.annotation_time = stream_item.stream_time

        ## build a Label for the doc-level label:
        rating = streamcorpus.Rating()
        rating.annotator = anno
        rating.target = streamcorpus.Target(target_id = target_id)
        rating.contains_mention = True

        ## heuristically split the mentions string on white space and
        ## use each token as a separate mention.
        rating.mentions = map(cleanse, mention.decode('utf-8').split())

        ## put this one label in the array of labels
        streamcorpus.add_annotation(stream_item, rating)

        ## provide this stream_item to the pipeline
        return stream_item

if __name__ == '__main__':
    ## this is a simple test of this extractor stage
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'input_dir',
        help='path to a yaml/tsv file with a specification of the corpus.')
    args = parser.parse_args()

    tsv_files_list_instance = tsv_files_list({})

    for si in tsv_files_list_instance( args.input_dir ):
        print len(si.body.raw), si.stream_id
