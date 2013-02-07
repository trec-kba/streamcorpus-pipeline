'''
Defines TaggerBatchTransform base class for wrapping taggers that
generate Tokens with automatic annotation.

This software is released under an MIT/X11 open source license.

Copyright 2012 Diffeo, Inc.
'''
import gc
import os
import sys
import time
import traceback
import itertools
import subprocess
import exceptions
import collections
import streamcorpus
import xml.dom.minidom
from streamcorpus import Chunk, Tagging, Label, OffsetType
from ._clean_visible import make_clean_visible_file, cleanse
from sortedcollection import SortedCollection

def make_chains_with_names(sentences):
    '''
    assemble in-doc coref chains by mapping equiv_id to tokens and
    their cleansed name strings

    :param sentences: iterator over token generators
    :returns dict:  
        keys are equiv_ids, 
        values are tuple(concatentated name string, list of tokens)
    '''
    ## if an equiv_id is -1, then the token is classified into some
    ## entity_type but has not other tokens in its chain.  We don't
    ## want these all lumped together, so we give them distinct "fake"
    ## equiv_id other than -1 -- counting negatively to avoid
    ## collisions with "real" equiv_ids
    fake_equiv_ids = -2
    
    ## use a default dictionary
    equiv_ids = collections.defaultdict(lambda: (set(), set()))

    for tagger_id, sents in sentences.items():
        for sent in sents:
            for tok in sent.tokens:
                if tok.entity_type is not None:

                    ## get an appropriate equiv_id
                    if tok.equiv_id == -1:
                        eqid = fake_equiv_ids
                        fake_equiv_ids -= 1
                    else:
                        eqid = tok.equiv_id

                    ## store the name parts initially as a set
                    equiv_ids[eqid][0].add(cleanse(tok.token.decode('utf8')))
                    ## 
                    equiv_ids[eqid][1].add(tok)

    ## convert name sets to strings
    for eqid in equiv_ids:
        equiv_ids[eqid] = ( 
            u' '.join( equiv_ids[eqid][0] ),
            equiv_ids[eqid][1] )

    return equiv_ids

def names_in_chains(stream_item, aligner_data):
    '''
    Add label to any token in a coref chain that has all the names

    :param stream_item: document that has a doc-level Rating to translate into token-level Labels.
    :param aligner_data: dict containing:
      names: list of strings to require
      annotator_id: string to find at stream_item.Ratings[i].annotator.annotator_id
    '''
    names        = aligner_data['names']
    annotator_id = aligner_data['annotator_id']

    label = None
    for rating in stream_item.ratings:
        if annotator_id == rating.annotator.annotator_id:
            label = Label(annotator=rating.annotator,
                          target_id=rating.target_id)
            break

    ## do nothing
    if label == None:
        #log something?
        return

    ## make inverted index names-->tokens
    equiv_ids = make_chains_with_names( stream_item.body.sentences )

    for eqid, sets in equiv_ids.items():
        ## detect 'smith' in 'smithye'
        found_one = True
        for name in names:
            if name not in sets[0]:
                found_one = False
                ## try next chain
                break

        if found_one:
            ## apply the label
            for tok in sets[1]:
                tok.labels.append( label )

    ## stream_item passed by reference, so nothing to return

def line_offset_labelsets(stream_item, aligner_data):
    ## get a set of tokens -- must have OffsetType.LINES in them.
    sentences = stream_item.body.sentences[aligner_data['tagger_id']]

    ## if given a LabelSet, then make labels on tokens
    for labelset in stream_item.body.labelsets:
        if labelset.annotator.annotator_id != aligner_data['annotator_id']:
            continue
        for label in labelset.labels:

            ## make sure to update the annotator; no check for this...?
            label.annotator = labelset.annotator

            off = label.offsets[OffsetType.LINES]
            assert off.length == len(off.value.split('\n'))
            #print 'L: %d\t%r\t%r' % (off.first, off.value, 
            #                         '\n'.join(hope_original.split('\n')[off.first:off.first+off.length]))

            ## These next few steps are probably the most
            ## memory intensive, because they fully
            ## instantiate all the tokens.

            token_collection = SortedCollection(
                itertools.chain(*[sent.tokens for sent in sentences]),
                key=lambda x: x.offsets[OffsetType.LINES].first
                )
            toks = token_collection.find_range(
                    off.first, off.first + off.length)
            for tok in toks:
                tok.labels.append( label )

                ## only for debugging
                if not tok.token in off.value:
                    sys.exit('%r not in %r' % \
                        ([(t.offsets[OffsetType.LINES].first, t.token) 
                          for t in toks], 
                         off.value))


AlignmentStrategies = {
    'names_in_chains': names_in_chains,
    'line_offset_labelsets': line_offset_labelsets,
    }

class TaggerBatchTransform(object):
    '''
    kba.pipeline.TaggerBatchTransform provides a structure for
    aligning a taggers output with labels and generating
    stream_item.sentences[tagger_id] = [Sentence]
    '''
    template = None

    def __init__(self, config):
        self.config = config

    def __call__(self, chunk_path):
        ## make temporary file paths based on chunk_path
        clean_visible_path = chunk_path + '-clean_visible.xml'
        ner_xml_path       = chunk_path + '-ner.xml'

        ## process the chunk's clean_visible data into xml
        i_chunk = Chunk(path=chunk_path, mode='rb')
        make_clean_visible_file(i_chunk, clean_visible_path)

        ## make sure holding nothing that consumes memory
        i_chunk = None

        ## generate an output file from the tagger
        self.make_ner_file(clean_visible_path, ner_xml_path)

        ## make a new output chunk at a temporary path
        tmp_chunk_path     = chunk_path + '_'
        o_chunk = Chunk(path=tmp_chunk_path, mode='wb')

        ## re-open i_chunk
        i_chunk = Chunk(path=chunk_path, mode='rb')

        ## fuse the output file with i_chunk to make o_chunk
        self.align_chunk_with_ner(ner_xml_path, i_chunk, o_chunk)

        ## clean up temp files
        if self.config['cleanup_tmp_files']:
            os.remove(clean_visible_path)
            os.remove(ner_xml_path)

        ## atomic rename new chunk file into place
        os.rename(tmp_chunk_path, chunk_path)

    ## gets called by self.__call__
    def make_ner_file(self, clean_visible_path, ner_xml_path):
        '''run tagger a child process to get XML output'''
        if self.template is None:
            raise exceptions.NotImplementedError('''
Subclasses must specify a class property "template" that provides
command string format for running a tagger.  It should take
%(pipeline_root)s as the path from the config file,
%(clean_visible_path)s as the input XML file, and %(ner_xml_path)s as
the output path to create.
''')
        cmd = self.template % dict(
            pipeline_root=self.config['pipeline_root'],
            clean_visible_path=clean_visible_path,
            ner_xml_path=ner_xml_path)
        print cmd
        start_time = time.time()
        ## make sure we are using as little memory as possible
        gc.collect()
        try:
            _child = subprocess.Popen(cmd, stderr=subprocess.PIPE, shell=True)
        except OSError, exc:
            print traceback.format_exc(exc)
            print('out of memory on:\n%r\n%r' % (clean_visible_path, ner_xml_path))
            from . import _memory
            print 'VmSize: %d bytes' % _memory.memory()
            print 'VmRSS:  %d bytes' % _memory.resident()
            print 'VmStk:  %d bytes' % _memory.stacksize()
            print 'uncollectable garbage: %r' % gc.garbage
            print 'gc.get_count() = %r' % repr(gc.get_count())
            sys.stdout.flush()
            ## Do not do gc.get_objects, because it causes nltk to
            ## load stuff that is not actually in memory yet!
            #print 'current objects: %r' % gc.get_objects()
            sys.exit(int(self.config['exit_code_on_out_of_memory']))

        s_out, errors = _child.communicate()
        assert _child.returncode == 0 and 'Exception' not in errors, errors
        elapsed = time.time() - start_time
        return elapsed
        #print '%.1f sec --> %.1f StreamItems/second' % (elapsed, rate)

    ## gets called by self.__call__
    def align_chunk_with_ner(self, ner_xml_path, i_chunk, o_chunk):
        ''' iterate through ner_xml_path to fuse with i_chunk into o_chunk '''
        ## prepare to iterate over the input chunk
        input_iter = i_chunk.__iter__()

        all_ner = xml.dom.minidom.parse(open(ner_xml_path))

        ## this converts our UTF-8 data into unicode strings, so when
        ## we want to compute byte offsets or construct tokens, we
        ## must .encode('utf8')
        for ner_dom in all_ner.getElementsByTagName('FILENAME'):
        #for stream_id, raw_ner in files(open(ner_xml_path).read().decode('utf8')):

            stream_item = input_iter.next()

            ## get stream_id out of the XML
            stream_id = ner_dom.attributes.get('stream_id').value
            if stream_item.stream_id is None:
                assert stream_id is None, 'out of sync: None != %r' % stream_id
                print('si.stream_id is None... ignoring')
                continue
            assert stream_id and stream_id == stream_item.stream_id, \
                '%s != %s' % (stream_id, stream_item.stream_id)

            if not stream_item.body:
                ## the XML better have had an empty clean_visible too...
                #assert not ner_dom....something
                continue

            tagging = Tagging()
            tagging.tagger_id = self.tagger_id

            '''
            ## get this one file out of its FILENAME tags
            tagged_doc_parts = list(files(ner_dom.toxml()))
            if not tagged_doc_parts:
                continue

            tagged_doc = tagged_doc_parts[0][1]

            ## hack
            hope_original = make_clean_visible(tagged_doc, '')
            open(ner_xml_path + '-clean', 'wb').write(hope_original.encode('utf-8'))
            print ner_xml_path + '-clean'
            '''

            #tagging.raw_tagging = tagged_doc
            tagging.generation_time = streamcorpus.make_stream_time()
            stream_item.body.taggings[self.tagger_id] = tagging

            ## could consume lots of memory here by instantiating everything
            stream_item.body.sentences[self.tagger_id] = list( self.get_sentences(ner_dom) )

            '''
            for num, sent in enumerate(sentences):
                for tok in sent.tokens:
                    print '%d\t%d\t%s' % (num, tok.offsets[OffsetType.LINES].first, repr(tok.token))
            '''

            if 'align_labels_by' in self.config and self.config['align_labels_by']:
                assert 'aligner_data' in self.config, 'config missing "aligner_data"'
                aligner = AlignmentStrategies[ self.config['align_labels_by'] ]
                aligner( stream_item, self.config['aligner_data'] )

            o_chunk.add(stream_item)

        ## all done, so close the o_chunk
        o_chunk.close()

    def get_sentences(self, ner_dom):
        '''parse the sentences and tokens out of the XML'''
        raise exceptions.NotImplementedError
