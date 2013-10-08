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
import stages
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
                    ## carry a *reference* to the entire Token object
                    equiv_ids[eqid][1].add(tok)

    return equiv_ids

def ALL_mentions(target_mentions, chain_mentions):
    '''
    For each name string in the target_mentions list, searches through
    all chain_mentions looking for any cleansed Token.token that
    contains the name.  Returns True only if all of the target_mention
    strings appeared as substrings of at least one cleansed
    Token.token.  Otherwise, returns False.

    :type target_mentions: list of basestring
    :type chain_mentions: list of basestring
    
    :returns bool:
    '''
    found_all = True
    for name in target_mentions:
        found_one = False
        for chain_ment in chain_mentions:
            if name in chain_ment:
                found_one = True
                break
        if not found_one:
            found_all = False
            break
    return found_all

def ANY_mentions(target_mentions, chain_mentions):
    '''
    For each name string in the target_mentions list, searches through
    all chain_mentions looking for any cleansed Token.token that
    contains the name.  Returns True if any of the target_mention
    strings appeared as substrings of any cleansed Token.token.
    Otherwise, returns False.

    :type target_mentions: list of basestring
    :type chain_mentions: list of basestring
    
    :returns bool:
    '''
    for name in target_mentions:
        for chain_ment in chain_mentions:
            if name in chain_ment:
                return True
    return False

_CHAIN_SELECTORS = dict(
    ALL = ALL_mentions,
    ANY = ANY_mentions,
    )

def names_in_chains(stream_item, aligner_data):
    '''
    Convert doc-level Rating object into a Label, and add that Label
    to all Token in all coref chains identified by
    aligner_data["chain_selector"]

    :param stream_item: document that has a doc-level Rating to translate into token-level Labels.
    :param aligner_data: dict containing:
      chain_selector: ALL or ANY
      annotator_id: string to find at stream_item.Ratings[i].annotator.annotator_id

    If chain_selector==ALL, then only apply Label to chains in which
    all of the Rating.mentions strings appear as substrings within at
    least one of the Token.token strings.

    If chain_selector==ANY, then apply Label to chains in which any of
    the Rating.mentions strings appear as a substring within at least
    one of the Token.token strings.
    '''
    chain_selector = aligner_data.get('chain_selector', '')
    assert chain_selector in _CHAIN_SELECTORS, \
        'chain_selector: %r not in %r' % (chain_selector, _CHAIN_SELECTORS.keys())

    ## convert chain_selector to a function
    chain_selector = _CHAIN_SELECTORS[chain_selector]

    ## make inverted index equiv_id --> (names, tokens)
    equiv_ids = make_chains_with_names( stream_item.body.sentences )

    for annotator_id, ratings in stream_item.ratings.items():        
        if annotator_id == aligner_data['annotator_id']:
            for rating in ratings:
                label = Label(annotator=rating.annotator,
                              target=rating.target)

                for eqid, (chain_mentions, chain_tokens) in equiv_ids.items():
                    if chain_selector(rating.mentions, chain_mentions):
                        ## apply the label
                        for tok in chain_tokens:
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

    ## These next few steps are probably the most
    ## memory intensive, because they fully
    ## instantiate all the tokens.

    token_collection = SortedCollection(
        itertools.chain(*[sent.tokens for sent in sentences]),
        key=lambda tok: tok.offsets[OffsetType.BYTES].first
        )

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

def make_memory_info_msg(clean_visible_path=None, ner_xml_path=None):
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

def align_labels(t_path1, config):
    if not ('align_labels_by' in config and config['align_labels_by']):
        return
    assert 'aligner_data' in config, 'config missing "aligner_data"'
    aligner = AlignmentStrategies[ config['align_labels_by'] ]
    _aligner_core(t_path1, aligner, config['aligner_data'])

def _aligner_core(t_path1, aligner, aligner_data):
    t_chunk1 = Chunk(t_path1, mode='rb')
    t_path2 = t_path1 + '-tmp-aligning'
    t_chunk2 = Chunk(t_path2, mode='wb')
    for si in t_chunk1:
        aligner( si, aligner_data )
        t_chunk2.add(si)
    t_chunk1.close()
    t_chunk2.close()

    logger.info('atomic rename: %r --> %r' % (t_path2, t_path1))
    os.rename(t_path2, t_path1)
    logger.debug('done renaming')


class _aligner_batch_transform(stages.BatchTransform):
    # aligner needs to be a single element tuple containing a function pointer,
    # because otherwise a function pointer assigned at class definition scope becomes
    # "bound" to the class and will be called with a suprious first 'self' argument.
    aligner = None
    config_require = ()

    def __init__(self, config):
        self.config = config
        for k in self.config_require:
            assert k in self.config

    def process_path(self, chunk_path):
        _aligner_core(chunk_path, self.aligner[0], self.config)

    def shutdown(self):
        pass


class name_align_labels(_aligner_batch_transform):
    '''
    requires config['chain_selector'] = ALL | ANY
    requires config['annotator_id'] (which person/org did manual labelling)
    '''
    aligner = (names_in_chains,)
    config_require = ('chain_selector', 'annotator_id')


class line_offset_align_labels(_aligner_batch_transform):
    '''
    requires config['annotator_id'] (which person/org did manual labelling)
    requires config['tagger_id'] (which software did tagging to process)
    '''
    aligner = (line_offset_labels,)
    config_require = ('annotator_id', 'tagger_id')


class byte_offset_align_labels(_aligner_batch_transform):
    '''
    requires config['annotator_id'] (which person/org did manual labelling)
    requires config['tagger_id'] (which software did tagging to process)
    '''
    aligner = (byte_offset_labels,)
    config_require = ('annotator_id', 'tagger_id')


class TaggerBatchTransform(stages.BatchTransform):
    '''
    streamcorpus.pipeline.TaggerBatchTransform provides a structure for
    aligning a taggers output with labels and generating
    stream_item.sentences[tagger_id] = [Sentence]
    '''
    template = None

    def __init__(self, config):
        self.config = config
        self._child = None

    def process_path(self, chunk_path):
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
        tagger_config = dict(
            pipeline_root_path=self.config['pipeline_root_path'],
            clean_visible_path=clean_visible_path,
            ner_xml_path=ner_xml_path)
        ## get a java_heap_size or default to 1GB
        tagger_config['java_heap_size'] = self.config.get('java_heap_size', '')
        cmd = self.template % tagger_config
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
                raise PipelineBaseException('tagger exited with %r' % self._child.returncode)

        elapsed = time.time() - start_time
        logger.info('finished tagging in %.1f seconds' % elapsed)
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
                logger.critical('si.stream_id is None... ignoring')
                continue
            assert stream_id and stream_id == stream_item.stream_id, \
                '%s != %s' % (stream_id, stream_item.stream_id)

            if not stream_item.body:
                ## the XML better have had an empty clean_visible too...
                #assert not ner_dom....something
                continue

            tagging = Tagging()
            tagging.tagger_id = self.tagger_id  # pylint: disable=E1101

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
            stream_item.body.taggings[self.tagger_id] = tagging       # pylint: disable=E1101

            ## could consume lots of memory here by instantiating everything
            sentences, relations, attributes = self.get_sentences(ner_dom)
            stream_item.body.sentences[self.tagger_id] = sentences    # pylint: disable=E1101
            stream_item.body.relations[self.tagger_id] = relations    # pylint: disable=E1101
            stream_item.body.attributes[self.tagger_id] = attributes  # pylint: disable=E1101 

            logger.debug('finished aligning tokens %s' % stream_item.stream_id)

            '''
            for num, sent in enumerate(sentences):
                for tok in sent.tokens:
                    print '%d\t%d\t%s' % (num, tok.offsets[OffsetType.LINES].first, repr(tok.token))
            '''

            if 'align_labels_by' in self.config and self.config['align_labels_by']:
                assert 'aligner_data' in self.config, 'config missing "aligner_data"'
                aligner = AlignmentStrategies[ self.config['align_labels_by'] ]
                aligner( stream_item, self.config['aligner_data'] )

            ## forcibly collect dereferenced objects
            gc.collect()

            try:
                o_chunk.add(stream_item)
            except MemoryError, exc:
                msg = traceback.format_exc(exc)
                msg += make_memory_info_msg()
                logger.critical(msg)
                raise PipelineOutOfMemory(msg)

        ## all done, so close the o_chunk
        try:
            o_chunk.close()
            logger.info('finished chunk for %r' % ner_xml_path)
        except MemoryError, exc:
            msg = traceback.format_exc(exc)
            msg += make_memory_info_msg()
            logger.critical(msg)
            raise PipelineOutOfMemory(msg)

    def get_sentences(self, ner_dom):
        '''parse the sentences and tokens out of the XML'''
        raise exceptions.NotImplementedError

    def shutdown(self):
        '''
        send SIGTERM to the tagger child process
        '''
        if self._child:
            try:
                self._child.terminate()
            except OSError, exc:
                if exc.errno == 3:
                    ## child is already gone, possibly because it ran
                    ## out of memory and caused us to shutdown
                    pass
