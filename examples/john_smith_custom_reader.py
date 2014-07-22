'''Example of how to transform a corpus into the streamcorpus format
and feed it into the streamcorpus_pipeline.  This offers a "reader"
stage that can be referenced in a streamcorpus_pipeline configuration
file.

This does the same thing as the regular streamcorpus_pipeline's
built-in john_smith reader with these differences:

1) this is run as an "external stage", see example:
configs/john-smith-with-labels-from-tsv.yaml

2) ground truth data is loaded from a tab-separated-values value,
which in this case is constructed from directory structure of
data/john-smith-original

3) also loads the expected mention strings from the TSV and cleanses
them, i.e. strips punctuation, lowercases, and splits on whitespace.
These strings are used by aligner stages after various taggers run,
e.g. LingPipe or Serif.

Copyright 2012-2014 Diffeo, Inc.
'''

## this assumes that streamcorpus has been installed
import streamcorpus

import os
import hashlib
import fnmatch
import logging
from streamcorpus_pipeline._clean_visible import cleanse
from streamcorpus_pipeline.stages import Configured

logger = logging.getLogger('streamcorpus_pipeline.john_smith_custom_reader')

class john_smith_custom_reader(Configured):
    config_name = 'john_smith_custom_reader'
    def __init__(self, *args, **kwargs):
        super(john_smith_custom_reader, self).__init__(*args, **kwargs)

        ## map input file names to ground truth data (used below)
        self.ground_truth = dict()

        ground_truth_path = self.config.get('ground_truth_path', None)
        if ground_truth_path:
            for line in open(ground_truth_path):
                target_id, mention, file_name, entity_type = line.split('\t')
                #logger.critical(file_name)
                self.ground_truth[file_name] = (mention, target_id)

    def __call__(self, path_to_original):
        '''
        This _looks_ like a Chunk only in that it generates StreamItem
        instances when iterated upon.
        '''

        if not path_to_original.startswith('/'):
            path_to_original = os.path.join(os.getcwd(), path_to_original)

        for root_dir, dirnames, filenames in os.walk(path_to_original):
            ## in this example, use a wildcard to match all files
            for fname in fnmatch.filter(filenames, '*'):
                dir_path = os.path.join(path_to_original, root_dir)
                yield self._make_stream_item(dir_path, fname)

    def _make_stream_item(self, dir_path, fname):

        ## could use dirpath as the label.  Instead, we illustrate
        ## using a TSV file to lookup the ground truth using the fname.
        assert fname in self.ground_truth, (dir_path, fname)

        ## "mention" is the name string from the text
        ## "target_id" is the label
        mention, target_id = self.ground_truth[fname]

        ## Every StreamItem has a stream_time property.  It usually comes
        ## from the document creation time.  Here, we assume the JS corpus
        ## was created at one moment at the end of 1998:
        creation_time = '1998-12-31T23:59:59.999999Z'

        stream_item = streamcorpus.make_stream_item(
            creation_time,
            ## make up an abs_url
            os.path.join(
                'john-smith-corpus', target_id, fname))

        ## These docs came from the authors of the paper cited above.
        stream_item.source = 'bagga-and-baldwin'

        ## build a ContentItem for the body
        body = streamcorpus.ContentItem()
        raw_string = open(os.path.join(dir_path, fname)).read()
        ## We know that this is already clean and has nothing
        ## tricky in it, because we manually cleansed it.  To
        ## illustrate how we stick all strings into thrift, we
        ## convert this to unicode (which introduces no changes)
        ## and then encode it as utf-8, which also introduces no
        ## changes.  Thrift stores strings as 8-bit character
        ## strings.
        # http://www.mail-archive.com/thrift-user@incubator.apache.org/msg00210.html
        body.clean_visible = unicode(raw_string).encode('utf8')

        ## attach the content_item to the stream_item
        stream_item.body = body

        stream_item.body.language = streamcorpus.Language(code='en', name='ENGLISH')

        ## The authors also annotated the corpus
        anno = streamcorpus.Annotator()
        anno.annotator_id = 'bagga-and-baldwin'
        anno.annotation_time = stream_item.stream_time

        ## build a Label for the doc-level label:
        rating = streamcorpus.Rating()
        rating.annotator = anno
        rating.target = streamcorpus.Target(target_id = target_id)
        rating.contains_mention = True

        ## heuristically split the mentions string on white space and
        ## use each token as a separate mention.  For other corpora,
        ## this might need to be more sophisticated.
        rating.mentions = map(cleanse, mention.decode('utf8').split())

        ## put this one label in the array of labels
        streamcorpus.add_annotation(stream_item, rating)

        ## provide this stream_item to the pipeline
        return stream_item


## this map allows this file to be specified in a config.yaml file as
## an external_stages_path
Stages = {
    'john_smith_custom_reader': john_smith_custom_reader,
    }


if __name__ == '__main__':
    ## this is a simple test of this reader stage
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'input_dir',
        help='path to a directory containing the original John Smith corpus.')
    args = parser.parse_args()

    john_smith_custom_reader_instance = Stages["john_smith_custom_reader"](
        dict(ground_truth_path='data/john-smith/ground-truth.tsv'))

    for si in john_smith_custom_reader_instance( args.input_dir ):
        print len(si.body.clean_visible), si.stream_id
