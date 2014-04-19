'''
Transforms a corpus into the streamcorpus format.

Data is loaded from a yaml file specifying the documents in the
corpus. The file should contain metadat and a list of documents in the
body. Example:

source: <source>
annotator_id: <annotator_id>
entities:
  - target_id: <target_id>
    external_profiles:
      - doc_path: <path to file>
        abs_url: http://...
    doc_path: <single path to base dir of wget --mirror directory tree>
      - doc_path: <path to base dir of wget --mirror directory tree>
      - doc_path: <path to base dir of wget --mirror directory tree>
      - doc_path: <path to file>
        abs_url: http://...
      - doc_path: <path to dir with little structure>
        abs_url: http://... <will append fragment: #urllib.quote(sub-path)>
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
import urllib

import magic
import yaml

import streamcorpus
from collections import defaultdict
from streamcorpus_pipeline._clean_visible import cleanse
from streamcorpus_pipeline.stages import Configured

logger = logging.getLogger(__name__)

class yaml_files_list(Configured):
    '''Read and tag flat files as described in a YAML file.

    When this reader stage is in use, the input file to the pipeline
    is a YAML file.  That YAML file points at several other files and
    directories, and the contents of those files are emitted as
    stream items.

    The YAML file has the following format:

    .. code-block:: yaml

        root_path: /data/directory
        annotator_id: my-name
        source: weblog
        entities:
          - target_id: "https://kb.diffeo.com/sample"
            doc_path:
              - sample
              - doc_path: copies/sample
                abs_url: "http://source.example.com/sample"
            external_profiles:
              - doc_path: wikipedia/Sample
                abs_url: "http://en.wikipedia.org/wiki/Sample"
            slots:
              - canonical_name: Sample
              - example

    At the top level, the file contains:

    ``root_path`` (default: current directory)
      Root path to the data to read.
    ``annotator_id`` (required)
      Annotator ID string on produced document ratings.
    ``source`` (default: ``unknown``)
      Source type for produced stream items.
    ``entities`` (required)
      List of entity document sets to read.

    ``entities`` is a list of dictionaries.  Each entity dictionary has
    the following items:

    ``target_id`` (required)
      Target entity URL for produced ratings
    ``doc_path`` (required)
      List of source files or directories.  These may be strings or
      dictionaries.  If dictionaries, ``doc_path`` is the filesystem
      path and ``abs_url`` is the corresponding URL; if strings, the
      strings are filesystem paths and the URL is generated.
      Filesystem paths are interpreted relative to the ``root_path``.
    ``external_profiles`` (optional)
      List of additional source files or directories that are external
      profiles, such as Wikipedia articles.  These are processed in
      the same way as items in ``doc_path`` but must be in dictionary
      form, where the ``abs_url`` field identifies the external
      knowledge base article.
    ``slots`` (required)
      Additional knowledge base metadata about the entity.  This is a
      list of strings or dictionaries.  Strings are interpreted as
      dictionaries of ``name`` mapping to the string.  The resulting
      slot dictionaries are combined into a multiset where each slot
      name maps to multiple values.

    For each entity, the reader finds all documents in the named
    directories and creates a :class:`streamcorpus.StreamItem` for
    each.  The :attr:`~streamcorpus.StreamItem.body` has its
    :attr:`~streamcorpus.ContentItem.raw` data set to the contents of
    the file.  The stream items are also tagged with a
    :class:`streamcorpus.Rating` where the annotator comes from the
    file metadata and the target comes from the entity ``target_id``.
    The :attr:`~streamcorpus.Rating.mentions` on the rating are set
    to the union of all of the slot values for all slots.

    '''
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

        if 'root_path' not in metadata:
            raise Exception('must specify root_path in yaml file')

        root_path = metadata['root_path']
        if root_path is None or len(root_path.strip()) == 0:
            ## just as in a regular pipeline config file, root_path
            ## empty means use current working dir.
            root_path = os.getcwd()

        if 'source' not in metadata:
            metadata['source'] = 'unknown'
            logger.warn('source not set, defaulting to unknown')

        if 'annotator_id' not in metadata:
            raise Exception('Invalid labels yaml file: must specify annotator_id')

        ## yaml file lists are organized *per* entity, however this
        ## must only emit each document once, even if multiple
        ## entities are mentioned in the text.  
        # dict keyed on absolute paths in local file system with
        # values as lists of entity records from the original input
        # yaml
        docs = defaultdict(list)
        abs_urls = dict()
        for entity in entities:
            ## get list of file paths to look at
            paths = entity['doc_path']
            if not isinstance(paths, list):
                paths = [paths]

            ## add any external profiles to the paths
            paths.extend(entity.get('external_profiles', []))

            ## TODO: put Flags on documents identified as profiles
            external_profiles = set()
            for ext_profile in entity.get('external_profiles', []):
                external_profiles.add(ext_profile['abs_url'])

            ## For this entity, only touch each documents once.
            ## Because the user can specify external_profiles and
            ## doc_paths we have the possibility of duplicates.
            visited_paths = []
            for path in paths:
                abs_url = None
                if isinstance(path, dict):
                    abs_url = path['abs_url']
                    path = path['doc_path']

                ## normalize path
                path = str(path)
                if not os.path.isabs(path):
                    path = os.path.join(root_path, path)

                ## recurse through directories
                if os.path.isdir(path):
                    for dirpath, dirnames, filenames in os.walk(path):
                        for fname in filenames:
                            fpath = os.path.join(dirpath, fname)
                            if fpath not in visited_paths:
                                is_profile = bool(fpath in external_profiles)
                                sub_path = fpath[len(path) + 1:]
                                if abs_url is None:
                                    ## construct a URL from the
                                    ## sub_path, which works with wget
                                    ## --mirror is used to fetch docs
                                    ## into a directory
                                    _abs_url = 'http://' + sub_path
                                else:
                                    ## display the individual files as
                                    ## url fragments, which handles
                                    ## the case of a zip archive that
                                    ## was manually unpacked 
                                    _abs_url = abs_url + '#' + urllib.quote(sub_path)
                                docs[fpath].append((entity, is_profile))
                                abs_urls[fpath] = _abs_url
                                visited_paths.append(fpath)
                else:
                    if path not in visited_paths:
                        is_profile = bool(path in external_profiles)
                        if abs_url is None:
                            raise Exception('when specifying a full path, you must also specify the abs_url: %r' % path)
                        docs[path].append((entity, is_profile))
                        abs_urls[path] = abs_url
                        visited_paths.append(path)

        ## multiple mentions per doc have been grouped
        for path in docs:
            yield self._make_stream_item(path, metadata, abs_urls[path], docs[path])
            

    @classmethod
    def _parse_slots(cls, raw_slots):
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

    @classmethod
    def _make_stream_item(cls, path, metadata, abs_url, entities):
        '''
        
        '''
        ## Every StreamItem has a stream_time property.  It usually comes
        ## from the document creation time.
        creation_time = os.path.getctime(path)

        ## make stream item
        stream_item = streamcorpus.make_stream_item(
            creation_time,
            abs_url)

        stream_item.source = metadata.get('source')

        ## build a ContentItem for the body
        body = streamcorpus.ContentItem()
        body.media_type = magic.from_file(path, mime=True)
        
        logger.info('opening %r', path)
        with open(path) as f:
            body.raw = f.read()

        ## attach the content_item to the stream_item
        stream_item.body = body

        ## annotations
        anno = streamcorpus.Annotator()
        anno.annotator_id = metadata['annotator_id']
        anno.annotation_time = stream_item.stream_time

        num_ratings = 0
        for entity, is_profile in entities:
            num_ratings += 1

            ## pull out target id and mention tokens
            target_id = str(entity['target_id'])

            ## build a Label for the doc-level label:
            rating = streamcorpus.Rating()
            rating.annotator = anno
            rating.target = streamcorpus.Target(target_id = target_id)
            rating.contains_mention = True

            if is_profile:
                rating.flags = [streamcorpus.FlagType.PROFILE]

            ## parse slots in yaml file
            slots = cls._parse_slots(entity['slots'])

            ## heuristically split the slots string on white space and
            ## use each token as a separate mention.
            rating.mentions = [cleanse(unicode(slot[1], 'utf-8')) for slot in slots]

            ## put this one label in the array of labels
            streamcorpus.add_annotation(stream_item, rating)

        ## provide this stream_item to the pipeline
        logger.info('created StreamItem(num ratings=%d, abs_url=%r', num_ratings, stream_item.abs_url)
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
