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
we have extractor output from LingPipe or Stanford CoreNLP, we coerce
these doc-level labels into labels on individual in-doc coref chains
that contain 'john' and 'smith' as substrings.

The original data is stored in data/john-smith/original and the output
of this file is in data/john-smith/john-smith.sc

Copyright 2012 Diffeo, Inc.
'''

## this assumes that streamcorpus has been installed
import streamcorpus

import os
import hashlib

def john_smith(config):
    '''
    Returns a kba.pipeline "extractor" that generates a single
    streamcorpus.Chunk file containing the John Smith corpus.
    '''
    def _john_smith(path_to_original):
        return generate_john_smith_chunk(path_to_original)
    
    return _john_smith

def generate_john_smith_chunk(path_to_original):
    '''
    
    '''
    ## assume JS corpus was created at one moment at the end of 1998
    creation_time = '1998-12-31T23:59:59.999999Z'

    if not path_to_original.startswith('/'):
        path_to_original = os.path.join(os.getcwd(), path_to_original)

    ## iterate over the files in the 35 input directories
    for label_id in range(35):

        dir_path = os.path.join(path_to_original, str(label_id))
        for fname in os.listdir(dir_path):

            stream_item = streamcorpus.make_stream_item(
                creation_time, 
                ## make up an abs_url
                os.path.join(
                    'john-smith-corpus', str(label_id), fname))

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

            ## The authors also annotated the corpus
            anno = streamcorpus.Annotator()
            anno.annotator_id = 'bagga-and-baldwin'
            anno.annotation_time = stream_item.stream_time

            ## build a Label for the doc-level label:
            rating = streamcorpus.Rating()
            rating.annotator = anno
            rating.target_id = str(label_id) # must be string
            rating.mentions = True

            ## put this one label in the array of labels
            stream_item.ratings.append( rating )

            ## provide this stream_item to the pipeline
            yield stream_item


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'input_dir', 
        help='path to a directory containing the original John Smith corpus.')
    parser.add_argument(
        'output_file', 
        help='file name for the output file to create -- must not exist yet.')
    args = parser.parse_args()
