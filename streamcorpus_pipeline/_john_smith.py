'''
Example of how to transform a corpus into the streamcorpus format.

This uses the John Smith corpus as a test data set for illustration.
The John Smith corpus is 197 articles from the New York Times gathered
by Amit Bagga and Breck Baldwin "Entity-Based Cross-Document
Coreferencing Using the Vector Space Model"
http://acl.ldc.upenn.edu/P/P98/P98-1012.pdf

The corpus consists of 35 directories with files inside each
directory.  The documents in each directory all refer to the same
entity named John Smith, so the directory names are document-level
labels.  First, we store these doc-level labels and then later, when
we have reader output from LingPipe or Stanford CoreNLP, we coerce
these doc-level labels into labels on individual in-doc coref chains
that contain 'john' and 'smith' as substrings.

The original data is stored in data/john-smith/original and the output
of this file is in data/john-smith/john-smith.sc

This software is released under an MIT/X11 open source license.

Copyright 2012-2013 Diffeo, Inc.
'''

## this assumes that streamcorpus has been installed
import streamcorpus
from streamcorpus_pipeline._exceptions import PipelineBaseException
from streamcorpus_pipeline.stages import Configured

import os
import hashlib

class john_smith(Configured):
    config_name = 'john_smith'
    # no other config

    def __call__(self, i_str):
        '''
        Returns a kba.pipeline "reader" that generates a single
        streamcorpus.Chunk file containing the John Smith corpus.

        :returns function:
        '''
        return generate_john_smith_chunk(i_str)

def generate_john_smith_chunk(path_to_original):
    '''
    This _looks_ like a Chunk only in that it generates StreamItem
    instances when iterated upon.
    '''
    ## Every StreamItem has a stream_time property.  It usually comes
    ## from the document creation time.  Here, we assume the JS corpus
    ## was created at one moment at the end of 1998:
    creation_time = '1998-12-31T23:59:59.999999Z'
    correct_time = 915148799

    if not os.path.isabs(path_to_original):
        path_to_original = os.path.join(os.getcwd(), path_to_original)

    ## iterate over the files in the 35 input directories
    for label_id in range(35):

        dir_path = os.path.join(path_to_original, str(label_id))
        fnames = os.listdir(dir_path)
        fnames.sort()
        for fname in fnames:

            stream_item = streamcorpus.make_stream_item(
                creation_time, 
                ## make up an abs_url
                os.path.join(
                    'john-smith-corpus', str(label_id), fname))

            if int(stream_item.stream_time.epoch_ticks) != correct_time:
                raise PipelineBaseException('wrong stream_time construction: %r-->%r != %r'\
                                            % (creation_time, stream_item.stream_time.epoch_ticks,
                                               correct_time))

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
            rating.target = streamcorpus.Target(target_id = str(label_id)) # must be string
            rating.contains_mention = True
            rating.mentions = ['john', 'smith']

            ## put this one label in the array of labels
            streamcorpus.add_annotation(stream_item, rating)

            ## provide this stream_item to the pipeline
            yield stream_item

if __name__ == '__main__':
    ## this is a simple test of this reader stage
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'input_dir', 
        help='path to a directory containing the original John Smith corpus.')
    args = parser.parse_args()
    
    john_smith_reader_stage = john_smith({})
    for si in john_smith_reader_stage( args.input_dir ):
        print len(si.body.clean_visible), si.stream_id
