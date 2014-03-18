'''
Transforms a corpus into the streamcorpus format.

Data is loaded from a yaml file specifying the documents in the
corpus. The file should contain metadat and a list of documents in the
body. Example:

abs_url_base: <abs_url_base>
source: <source>
annotator_id: <annotator_id>
entities:
  - target_id: <target_id>
    profile_path:
        - <file>
        - <file>
        - <directory>
        - <directory>
    doc_path: <directory_path_or_list_of_files>
    slots:
      - slot-name: slot_value
      - <token>
      - <token> ... <token>
  - [...]

Copyright 2012-2014 Diffeo, Inc.
'''
from __future__ import absolute_import
import logging
import os

import magic
import yaml

import streamcorpus
import collections
from streamcorpus_pipeline._clean_visible import cleanse
from streamcorpus_pipeline.stages import Configured

logger = logging.getLogger(__name__)

class yaml_files_list(Configured):
    config_name = 'yaml_files_list'
    def __call__(self, path_to_input_file):
        '''
        This _looks_ like a Chunk only in that it generates StreamItem
        instances when iterated upon.
        '''
        path_to_input_file = os.path.abspath(path_to_input_file)

        with open(path_to_input_file) as f:
            ## read yaml data
            metadata = yaml.load(f)
            entities = metadata.pop('entities')

        base_dir = os.path.dirname(path_to_input_file)

        if 'root_path' in metadata:
            root_path = metadata['root_path']
            if os.path.isabs(root_path):
                base_dir = root_path
            else:
                base_dir = os.path.join(base_dir, root_path)

        if 'abs_url_base' not in metadata:
            metadata['abs_url_base'] = path_to_input_file
            logger.warn('abs_url_base not set, defaulting to %s' % path_to_input_file)

        if 'source' not in metadata:
            metadata['source'] = 'unknown'
            logger.warn('source not set, defaulting to unknown')

        if 'annotator_id' not in metadata:
            metadata['annotator_id'] = 'unknown'
            logger.warn('annotator_id not set, defaulting to unknown')

        for entity in entities:
            ## get list of file paths to look at
            paths = entity['doc_path']
            if not isinstance(paths, list):
                paths = [paths]

            ## For now just treat profiles the same as all other
            ## documents about an entity. Eventually we will
            ## somehow use the designated profiles differently.
            external_profiles = collections.defaultdict(str)
            if 'external_profiles' in entity:
                for external_profile in entity['external_profiles']:
                    ## normalize path
                    path = str(path)
                    if not os.path.isabs(path):
                        path = os.path.join(base_dir, path)
                    external_profiles[path] = external_profile['abs_url']
                paths.extend(external_profiles.keys())

            ## Only yield documents once.  Because the user can specify
            ## profile_paths and doc_paths we have the possibility of
            ## duplicates.
            visited_paths = []
            for path in paths:
                ## normalize path
                path = str(path)
                if not os.path.isabs(path):
                    path = os.path.join(base_dir, path)

                ## recurse through directories
                if os.path.isdir(path):
                    for dirpath, dirnames, filenames in os.walk(path):
                        for fname in filenames:
                            fpath = os.path.join(base_dir, dirpath, fname)
                            if fpath not in visited_paths:
                                yield self._make_stream_item(fpath, entity, metadata, external_profiles[fpath], fpath in external_profiles)
                                visited_paths.append(fpath)
                else:
                    if path not in visited_paths:
                        yield self._make_stream_item(path, entity, metadata, external_profiles[path], path in external_profiles)
                        visited_paths.append(path)

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

    def _parse_slots(self, raw_slots):
        ## raw_slots is a list containing either dictionaries
        ## or strings.
        ## Example:
        ## - slot-name: slot_value
        ## - name: APT1
        ## - APT1

        slots = []
        for mention in raw_slots:
            if isinstance(mention, dict):
                slots.extend(mention.iteritems())
            else:
                slots.append(('name', mention))
        return slots


    def _make_stream_item(self, path, entity, metadata, abs_url, is_profile):
        ## pull out target id and mention tokens
        target_id = str(entity['target_id'])

        ## parse slots in yaml file
        slots = self._parse_slots(entity['slots'])

        ## Every StreamItem has a stream_time property.  It usually comes
        ## from the document creation time.
        creation_time = os.path.getctime(path)

        ## construct abs_url
        ## SJB: We need something more sane than what is here.
        if not abs_url:
            if not metadata['abs_url_base']:
                abs_url_base = ''
            else:
                abs_url_base = metadata['abs_url_base']
            abs_url = os.path.join(
                abs_url_base,
                path)
            abs_url = self._ensure_unique(abs_url)

        ## make stream item
        stream_item = streamcorpus.make_stream_item(
            creation_time,
            abs_url)

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
        if is_profile:
            rating.flags = [streamcorpus.FlagType.PROFILE]

        ## heuristically split the slots string on white space and
        ## use each token as a separate mention.
        rating.mentions = [cleanse(unicode(slot[1], 'utf-8')) for slot in slots]

        ## put this one label in the array of labels
        streamcorpus.add_annotation(stream_item, rating)

        ## provide this stream_item to the pipeline
        return stream_item

if __name__ == '__main__':
    ## this is a simple test of this reader stage
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'input_dir',
        help='path to a yaml file with a specification of the corpus.')
    args = parser.parse_args()

    yaml_files_list_instance = yaml_files_list({})

    for si in yaml_files_list_instance( args.input_dir ):
        print len(si.body.raw), si.stream_id
