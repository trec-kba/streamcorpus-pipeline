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
import _stages
import _memory
import logging
import traceback
import itertools
import subprocess
import exceptions
import collections
import streamcorpus
import xml.dom.minidom
from streamcorpus import Chunk, Tagging, Label, OffsetType, add_annotation
from _clean_visible import make_clean_visible_file, cleanse
from sortedcollection import SortedCollection
from _exceptions import PipelineOutOfMemory, PipelineBaseException

logger = logging.getLogger(__name__)

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
    names = aligner_data['names']

    for annotator_id, ratings in stream_item.ratings.items():        
        if annotator_id == aligner_data['annotator_id']:
            for rating in ratings:
                label = Label(annotator=rating.annotator,
                              target=rating.target)

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
                            add_annotation(tok, label)

                ## stream_item passed by reference, so nothing to return

def line_offset_labels(stream_item, aligner_data):
    ## get a set of tokens -- must have OffsetType.LINES in them.
    sentences = stream_item.body.sentences[aligner_data['tagger_id']]

    ## if labels on ContentItem, then make labels on Tokens
    for annotator_id in stream_item.body.labels:
        if annotator_id != aligner_data['annotator_id']:
            continue
        for label in stream_item.body.labels[annotator_id]:

            ## remove the offset from the label, because we are
            ## putting it into the token
            label_off = label.offsets.pop(OffsetType.LINES)

            assert label_off.length == len(label_off.value.split('\n'))
            #print 'L: %d\t%r\t%r' % (label_off.first, label_off.value, 
            #    '\n'.join(hope_original.split('\n')[label_off.first:
            #         label_off.first+label_off.length]))

            ## These next few steps are probably the most
            ## memory intensive, because they fully
            ## instantiate all the tokens.
            token_collection = SortedCollection(
                itertools.chain(*[sent.tokens for sent in sentences]),
                key=lambda tok: tok.offsets[OffsetType.LINES].first
                )

            toks = token_collection.find_range(
                    label_off.first, label_off.first + label_off.length)

            for tok in toks:
                add_annotation(tok, label)

                ## only for debugging
                if not tok.token or tok.token not in label_off.value:
                    sys.exit('%r not in %r' % \
                        ([(t.offsets[OffsetType.LINES].first, t.token) 
                          for t in toks], 
                         label_off.value))

def byte_offset_labels(stream_item, aligner_data):
    ## get a set of tokens -- must have OffsetType.BYTES type offsets.
    sentences = stream_item.body.sentences[aligner_data['tagger_id']]

    ## if labels on ContentItem, then make labels on Tokens
    for annotator_id in stream_item.body.labels:
        if annotator_id != aligner_data['annotator_id']:
            continue
        for label in stream_item.body.labels[annotator_id]:

            ## remove the offset from the label, because we are
            ## putting it into the token
            label_off = label.offsets.pop( OffsetType.BYTES )

            assert label_off.length == len(label_off.value)
            
            #print 'L: %d\t%r\t%r' % (label_off.first, label_off.value, 
            #                         '\n'.join(hope_original.split('\n')[label_off.first:label_off.first+label_off.length]))

            ## These next few steps are probably the most
            ## memory intensive, because they fully
            ## instantiate all the tokens.

            token_collection = SortedCollection(
                itertools.chain(*[sent.tokens for sent in sentences]),
                key=lambda tok: tok.offsets[OffsetType.BYTES].first
                )

            #print 'tc %d %r' % (len(token_collection), token_collection._keys)
            #print 'label_off.first=%d, length=%d, value=%r' % (label_off.first, label_off.length, label_off.value)

            toks = token_collection.find_range(
                    label_off.first, label_off.first + label_off.length)

            #print "find_le: ", token_collection.find_le(label_off.first)

            toks = list(toks)
            #print 'aligned tokens', toks

            for tok in toks:
                add_annotation(tok, label)

                ## only for debugging
                assert tok.token is not None, tok.token

                if not tok.token in label_off.value:
                    sys.exit('%r not in %r' % \
                        ([(t.offsets[OffsetType.BYTES].first, t.token) 
                          for t in toks], 
                         label_off.value))

def make_memory_info_msg(clean_visible_path, ner_xml_path):
    msg = 'out of memory on:\n%r\n%r' % (clean_visible_path, ner_xml_path)
    msg += 'VmSize: %d bytes' % _memory.memory()
    msg += 'VmRSS:  %d bytes' % _memory.resident()
    msg += 'VmStk:  %d bytes' % _memory.stacksize()
    msg += 'uncollectable garbage: %r' % gc.garbage
    msg += 'gc.get_count() = %r' % repr(gc.get_count())
    ## Do not do gc.get_objects, because it causes nltk to
    ## load stuff that is not actually in memory yet!
    #msg += 'current objects: %r' % gc.get_objects()
    return msg

AlignmentStrategies = {
    'names_in_chains': names_in_chains,
    'line_offset_labels': line_offset_labels,
    'byte_offset_labels': byte_offset_labels,
    }

class TaggerBatchTransform(_stages.BatchTransform):
    '''
    kba.pipeline.TaggerBatchTransform provides a structure for
    aligning a taggers output with labels and generating
    stream_item.sentences[tagger_id] = [Sentence]
    '''
    template = None

    def __init__(self, config):
        self.config = config
        self._child = None

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
%(pipeline_root_path)s as the path from the config file,
%(clean_visible_path)s as the input XML file, and %(ner_xml_path)s as
the output path to create.
''')
        cmd = self.template % dict(
            pipeline_root_path=self.config['pipeline_root_path'],
            clean_visible_path=clean_visible_path,
            ner_xml_path=ner_xml_path)
        logger.critical( cmd )
        start_time = time.time()
        ## make sure we are using as little memory as possible
        gc.collect()
        try:
            self._child = subprocess.Popen(cmd, stderr=subprocess.PIPE, shell=True)
        except OSError, exc:
            msg = traceback.format_exc(exc) 
            msg += make_memory_info_msg(clean_visible_path, ner_xml_path)
            # instead of sys.ext, do proper shutdown
            #sys.exit(int(self.config['exit_code_on_out_of_memory']))
            raise PipelineOutOfMemory(msg)

        s_out, errors = self._child.communicate()
        if not self._child.returncode == 0:
            if 'java.lang.OutOfMemoryError' in errors:
                msg = errors + make_memory_info_msg(clean_visible_path, ner_xml_path)
                raise PipelineOutOfMemory(msg)
            elif self._child.returncode == 137:
                msg = 'tagger returncode = 137\n' + errors
                msg += make_memory_info_msg(clean_visible_path, ner_xml_path)
                # maybe get a tail of /var/log/messages
                raise PipelineOutOfMemory(msg)
            elif 'Exception' in errors:
                raise PipelineBaseException(errors)
            else:
                raise PipelineBaseException('tagger exitted with %r' % self._child.returncode)

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
                assert not stream_id, 'out of sync: None != %r' % stream_id
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

    def shutdown(self):
        '''
        send SIGTERM to the tagger child process
        '''
        if self._child:
            self._child.terminate()
