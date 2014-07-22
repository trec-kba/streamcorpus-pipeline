'''Example of how to transform a corpus into the streamcorpus format
and save it as a chunk file.

~/streamcorpus-pipeline$ python examples/john_smith_chunk_writer.py data/john-smith/original foo.sc
~/streamcorpus-pipeline$ streamcorpus_dump --count foo.sc
197     0       foo.sc

Copyright 2012-2014 Diffeo, Inc.
'''

import argparse
import logging
import os

## this assumes that streamcorpus has been installed; it is in pypi
import streamcorpus

logger = logging.getLogger(__name__)

def paths(input_dir):
    'yield all file paths under input_dir'
    for root, dirs, fnames in os.walk(input_dir):
        for i_fname in fnames:
            i_path = os.path.join(root, i_fname)
            yield i_path

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'input_dir',
        help='path to a directory containing files to put in a streamcorpus.Chunk file')
    parser.add_argument(
        'output_path',
        help='file path to create the Chunk file')
    args = parser.parse_args()

    o_fh = open(args.output_path, 'wb')
    o_chunk = streamcorpus.Chunk(file_obj=o_fh, mode='wb')

    ## NB: can also pass a path into Chunk.  If it ends in .xz, then
    ## it will handle file compress for you.
    #o_chunk = streamcorpus.Chunk('output.sc.xz', mode='wb')

    for i_path in paths(args.input_dir):
        ### In the example code below, strings that start with
        ### 'headers.' should be replaced by code that pulls the
        ### indicated information from scrapy 

        ## Every StreamItem has a stream_time property.  It usually
        ## comes from the document creation time.  This may be either
        ## a unix-time number or a string like
        creation_time = '2000-01-01T12:34:00.000123Z'  ## should come from headers.last_modified

        stream_item = streamcorpus.make_stream_item(
            creation_time,
            'headers.absolute URL')

        ## These docs came from the authors of the paper cited above.
        stream_item.source = 'scrapy'

        i_file = open(i_path)
        stream_item.body.raw = i_file.read()

        stream_item.body.media_type = 'headers.mime_type'
        stream_item.body.encoding = 'headers.encoding'

        o_chunk.add(stream_item)

    o_chunk.close()



if __name__ == '__main__':
    main()
