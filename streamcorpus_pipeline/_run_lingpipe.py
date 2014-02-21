#!/usr/bin/python
'''
Takes a directory of streamcorpus.Chunk files as input, builds
'cleansed' for body and each other_content if needed, puts the
cleansed into XML format expected by wrappers such as the LingPipe,
the kba-stanford-corenlp wrapper, and similar, parses output from one
of those systems, puts the data into Token arrays in Sentence objects,
converts to sentence_blobs, and writes out the new Chunk.

This is designed to run in Condor, which means that it needs to be
ready to get killed and moved to a new compute slot at any time.  To
cope with this, it finishes each step in temp files before doing an
atomic move to put the final product into position.

This software is released under an MIT/X11 open source license.

Copyright 2012-2013 Diffeo, Inc.
'''

import streamcorpus
from streamcorpus import Chunk, Label, Tagging
#import lingpipe
lingpipe = None

import os
import re
import uuid
import time
import string
import hashlib
import traceback
import subprocess
import collections
import xml.dom.minidom

pipeline_cmd_templates = {
    'stanford': 'java -Xmx2048m -jar %(PIPELINE_ROOT)s/runNER.jar %(INPUT_FILE)s %(OUTPUT_FILE)s',
    'lingpipe': 'cd %(PIPELINE_ROOT)s/lingpipe-4.1.0/demos/generic/bin && cat %(INPUT_FILE)s | ./cmd_coref_en_news_muc6.sh "-contentType=text/html" "-includeElts=FILENAME" 1> %(OUTPUT_FILE)s',
}

postproc_cmd_templates = {
    'stanford': 'mv %(RAW_OUTPUT_FILE)s %(OUTPUT_FILE)s',
    'lingpipe': 'python -m kba.pipeline.lingpipe postproc %(RAW_OUTPUT_FILE)s %(OUTPUT_FILE)s',
}

stream_id_re = re.compile('<FILENAME\s+id="(.*?)"')

def make_cleansed_file(i_chunk, tmp_cleansed_path):
    '''make a temp file of cleansed text'''
    tmp_cleansed = open(tmp_cleansed_path, 'wb')
    for idx, si in enumerate(i_chunk):
        tmp_cleansed.write('<FILENAME docid="%s">\n' % si.stream_id)
        tmp_cleansed.write(si.body.cleansed)
        ## how to deal with other_content?
        tmp_cleansed.write('</FILENAME>\n')
    tmp_cleansed.close()
    ## replace this with log.info()
    print 'created %s' % tmp_cleansed_path

def make_ner_file(tagger_id, tmp_cleansed_path, tmp_ner_path, pipeline_root):
    '''run child process to get OWPL output'''
    params = dict(INPUT_FILE=tmp_cleansed_path,
                  #RAW_OUTPUT_FILE=tmp_ner_raw_path,
                  OUTPUT_FILE=tmp_ner_path,
                  PIPELINE_ROOT=pipeline_root)
    
    pipeline_cmd = pipeline_cmd_templates[tagger_id] % params

    print pipeline_cmd

    ## replace this with log.info()
    print 'creating %s' % tmp_ner_path
    start_time = time.time()
    gpg_child = subprocess.Popen(
        pipeline_cmd,
        stderr=subprocess.PIPE, shell=True)
    s_out, errors = gpg_child.communicate()
    assert gpg_child.returncode == 0 and 'Exception' not in errors, errors
    elapsed = time.time() - start_time
    ## replace this with log.info()
    print 'created %s in %.1f sec' % (tmp_ner_path, elapsed)

    '''
    postproc_cmd = postproc_cmd_templates[tagger_id] % params

    print postproc_cmd

    ## replace this with log.info()
    print 'creating %s' % tmp_ner_raw_path
    start_time = time.time()
    gpg_child = subprocess.Popen(
        postproc_cmd,
        stderr=subprocess.PIPE, shell=True)
    s_out, errors = gpg_child.communicate()
    assert gpg_child.returncode == 0 and 'Exception' not in errors, errors
    elapsed = time.time() - start_time

    ## replace this with log.info()
    print 'created %s in %.1f sec' % (tmp_ner_path, elapsed)
    '''

## 'cleanse' 
whitespace = re.compile('''(\s|\n)+''')
strip_punctuation = string.maketrans(string.punctuation,
                                     ' ' * len(string.punctuation))

def cleanse(span):
    '''Convert a string of text into a lowercase string with no
    punctuation and only spaces for whitespace.

    :param span: string
    '''
    try:
        ## attempt to force it to utf8, which might fail
        span = span.encode('utf8', 'ignore')
    except:
        pass
    ## lowercase, strip punctuation, and shrink all whitespace
    span = span.lower()
    span = span.translate(strip_punctuation)
    span = whitespace.sub(' ', span)
    ## trim any leading or trailing whitespace
    return span.strip()


def align_chunk_with_ner(tmp_ner_path, i_chunk, tmp_done_path):
    '''
    iterate through the i_chunk and tmp_ner_path to generate a new
    Chunk with body.ner
    '''
    o_chunk = Chunk()
    input_iter = i_chunk.__iter__()
    ner = ''
    stream_id = None

    all_ner = xml.dom.minidom.parse(open(tmp_ner_path))

    for raw_ner in all_ner.getElementsByTagName('FILENAME'):
        
        stream_item = input_iter.next()
        ## get stream_id out of the XML
        stream_id = raw_ner.attributes.get('docid').value
        assert stream_id and stream_id == stream_item.stream_id, \
            '%s != %s\nner=%r' % (stream_id, stream_item.stream_id, ner)

        tagger_id = 'lingpipe'
        tagging = Tagging()
        tagging.tagger_id = tagger_id
        ## get this one file out of its FILENAME tags
        tagged_doc = list(lingpipe.files(raw_ner.toxml()))[0][1]
        tagging.raw_tagging = tagged_doc
        tagging.generation_time = streamcorpus.make_stream_time()
        stream_item.body.taggings[tagger_id] = tagging

        sentences = list(lingpipe.sentences(tagged_doc))

        ## make JS labels on individual tokens
        assert stream_item.ratings[0].mentions, stream_item.stream_id
        john_smith_label = Label()
        john_smith_label.annotator = stream_item.ratings[0].annotator
        john_smith_label.target_id = stream_item.ratings[0].target_id

        # first map all corefchains to their words
        equiv_ids = collections.defaultdict(lambda: set())
        for sent in sentences:
            for tok in sent.tokens:
                if tok.entity_type is not None:
                    equiv_ids[tok.equiv_id].add(cleanse(tok.token))

        ## find all the chains that are John Smith
        johnsmiths = set()
        for equiv_id, names in equiv_ids.items():
            ## detect 'smith' in 'smithye'
            _names = cleanse(' '.join(names))
            if 'john' in _names and 'smith' in _names:
                johnsmiths.add(equiv_id)

        print len(johnsmiths)
        ## now apply the label
        for sent in sentences:
            for tok in sent.tokens:
                if tok.equiv_id in johnsmiths:
                    tok.labels = [john_smith_label]                

        stream_item.body.sentences[tagger_id] = sentences
        
        o_chunk.add(stream_item)

    ## put the o_chunk bytes into the specified file
    open(tmp_done_path, 'wb').write(str(o_chunk))
    ## replace this with log.info()
    print 'created %s' % tmp_done_path

def run_pipeline(tagger_id, input_dir, output_dir, tmp_dir='/tmp', pipeline_root='./'):
    '''
    '''
    ## make tmp_dir more
    tmp_dir = os.path.join(tmp_dir, '%s-%s' % (uuid.uuid1(), os.getpid()))
    assert not os.path.exists(tmp_dir), tmp_dir
    os.makedirs(tmp_dir)
    
    for fname in os.listdir(input_dir):
        if not fname.endswith('.sc'):
            ## ignore any non streamcorpus.Chunk files
            continue

        fpath = os.path.join(input_dir, fname)

        ## just need one chunk for this tiny corpus
        i_chunk = Chunk(file_obj=open(fpath))

        ## prepare to make intermediate files in tmp_dir
        tmp_cleansed_path = os.path.join(tmp_dir, fname + '.cleansed.xml')
        tmp_ner_path      = os.path.join(tmp_dir, fname + '.ner.xml')
        tmp_done_path     = os.path.join(output_dir, fname + '.done.partial')
        final_done_path   = os.path.join(output_dir, fname + '.done.sc')

        make_cleansed_file(i_chunk, tmp_cleansed_path)

        make_ner_file(tagger_id, tmp_cleansed_path, tmp_ner_path, pipeline_root)

        align_chunk_with_ner(tmp_ner_path, i_chunk, tmp_done_path)

        ## atomic rename when done
        os.rename(tmp_done_path, final_done_path)

        ## replace with log.info()
        print 'done with %s' % final_done_path

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('tagger_id', 
                        metavar='stanford|lingpipe', 
                        help='name of NLP pipeline to run')
    parser.add_argument('input_dir', help='directory of Chunk files')
    parser.add_argument('output_dir', help='directory to put new streamcorpus.Chunk files')
    parser.add_argument('--convert-kba', action='store_true', default=False,
                        help='Expect input_dir to have old-style KBA chunks')
    parser.add_argument('--pipeline-root', metavar='PIPELINE_ROOT', dest='pipeline_root',
                        help='file path to root dir for the particular NLP pipeline')
    parser.add_argument('--align-only', metavar='OUTPUT_PATH', dest='align_only', default=None,
                        help='produce a chunk file at OUTPUT_PATH by alignning a single input chunk file with the intermediate')
    ## could add options to delete input after verifying the output on disk?
    args = parser.parse_args()

    if args.align_only is not None:
        i_chunk = Chunk(file_obj=open(args.input_dir))
        tmp_ner_path = args.output_dir
        tmp_done_path = args.align_only
        align_chunk_with_ner(tmp_ner_path, i_chunk, tmp_done_path)

    else:
        run_pipeline(args.tagger_id, args.input_dir, args.output_dir, pipeline_root=args.pipeline_root)
