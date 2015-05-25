'''
Defines TaggerBatchTransform base class for wrapping taggers that
generate Tokens with automatic annotation.

This software is released under an MIT/X11 open source license.

Copyright 2012-2013 Diffeo, Inc.
'''
from __future__ import absolute_import
import collections
import exceptions
import gc
import itertools
import logging
import os
import shutil
import subprocess
import sys
import time
import traceback

import regex as re

import xml.dom.minidom

import streamcorpus
from streamcorpus import Chunk, Tagging, Label, OffsetType, add_annotation
from streamcorpus_pipeline._clean_visible import make_clean_visible_file, \
    cleanse
from sortedcollection import SortedCollection
from streamcorpus_pipeline._exceptions import PipelineOutOfMemory, \
    PipelineBaseException, InvalidStreamItem
import streamcorpus_pipeline._memory as _memory
import streamcorpus_pipeline.stages
from yakonfig import ConfigurationError

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


def ANY_MULTI_TOKEN_mentions(multi_token_target_mentions, chain_mentions):
    '''
    For each name string (potentially consisting of multiple tokens) in the
    target_mentions list, searches through all chain_mentions looking for any
    cleansed Token.token that contains all the tokens in the name.  Returns
    True only if all of the target_mention strings appeared as substrings of at
    least one cleansed Token.token.  Otherwise, returns False.

    :type target_mentions: list of basestring
    :type chain_mentions: list of basestring

    :returns bool:
    '''
    for multi_token_name in multi_token_target_mentions:
        if ALL_mentions(multi_token_name.split(), chain_mentions):
            return True
    return False


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
    ANY_MULTI_TOKEN = ANY_MULTI_TOKEN_mentions,
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

    If chain_selector==ANY_MULTI_TOKEN, then apply Label to chains in which all
    the names in any of the Rating.mentions strings appear as a substring within at least
    one of the Token.token strings.
    '''
    chain_selector = aligner_data.get('chain_selector', '')
    assert chain_selector in _CHAIN_SELECTORS, \
        'chain_selector: %r not in %r' % (chain_selector, _CHAIN_SELECTORS.keys())

    ## convert chain_selector to a function
    chain_selector = _CHAIN_SELECTORS[chain_selector]

    ## make inverted index equiv_id --> (names, tokens)
    equiv_ids = make_chains_with_names( stream_item.body.sentences )

    required_annotator_id = aligner_data.get('annotator_id')

    for annotator_id, ratings in stream_item.ratings.items():
        if (required_annotator_id is not None) and (annotator_id != required_annotator_id):
            continue
        else:
            for rating in ratings:
                label = Label(annotator=rating.annotator,
                              target=rating.target)

                for eqid, (chain_mentions, chain_tokens) in equiv_ids.items():
                    if chain_selector(rating.mentions, chain_mentions):
                        ## apply the label
                        for tok in chain_tokens:
                            add_annotation(tok, label)

                ## stream_item passed by reference, so nothing to return

def _get_tagger_id(stream_item, aligner_data):
    available_taggings = stream_item.body.sentences.keys()
    if len(available_taggings) == 1:
        default_tagger_id = available_taggings[0]
    else:
        default_tagger_id = None
    tagger_id = aligner_data.get('tagger_id', default_tagger_id)
    if not tagger_id:
        raise Exception('need tagger_id, available taggings: %r', available_taggings)
    return tagger_id


def line_offset_labels(stream_item, aligner_data):
    ## get a set of tokens -- must have OffsetType.LINES in them.
    tagger_id = _get_tagger_id(stream_item, aligner_data)
    sentences = stream_item.body.sentences.get(tagger_id, [])

    required_annotator_id = aligner_data.get('annotator_id')

    ## if labels on ContentItem, then make labels on Tokens
    for annotator_id in stream_item.body.labels:
        if (required_annotator_id is not None) and (annotator_id != required_annotator_id):
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
                    raise InvalidStreamItem(
                        '%r not in %r' %
                        ([(t.offsets[OffsetType.LINES].first, t.token)
                          for t in toks],
                         label_off.value))

def byte_offset_labels(stream_item, aligner_data):
    _offset_labels(stream_item, aligner_data, offset_type='BYTES')

def char_offset_labels(stream_item, aligner_data):
    _offset_labels(stream_item, aligner_data, offset_type='CHARS')

def _offset_labels(stream_item, aligner_data, offset_type='BYTES'):
    ## get a set of tokens -- must have OffsetType.<offset_type> type offsets.

    otype_str = offset_type
    offset_type = OffsetType._NAMES_TO_VALUES[offset_type]

    tagger_id = _get_tagger_id(stream_item, aligner_data)
    sentences = stream_item.body.sentences.get(tagger_id, [])

    ## These next few steps are probably the most
    ## memory intensive, because they fully
    ## instantiate all the tokens.

    token_collection = SortedCollection(
        itertools.chain(*[sent.tokens for sent in sentences]),
        key=lambda tok: tok.offsets[offset_type].first
        )

    required_annotator_id = aligner_data.get('annotator_id')

    ## if labels on ContentItem, then make labels on Tokens
    for annotator_id in stream_item.body.labels:
        if (required_annotator_id is not None) and (annotator_id != aligner_data['annotator_id']):
            continue
        for label in stream_item.body.labels[annotator_id]:
            if offset_type not in label.offsets:
                # This was causing a very small number of jobs to fail in
                # the WLC dataset. Namely, the expected offset type did not
                # exist in `label.offsets`. (There was a byte offset but not
                # a char offset, even though the `char_offset_align_labels`
                # was used.) I don't understand enough about the system
                # to know why the expected offset type didn't exist, but it was
                # happening in few enough cases that I did feel comfortable
                # just ignoring it.
                # ---AG
                found_types = [OffsetType._VALUES_TO_NAMES.get(k, '???')
                               for k in label.offsets.keys()]
                logger.warn('Could not find offset type "%s" in label '
                            'offsets. Found %s types in stream item %r'
                            % (otype_str, found_types, stream_item.stream_id))
                continue

            ## remove the offset from the label, because we are
            ## putting it into the token
            label_off = label.offsets.pop( offset_type )

            if offset_type == OffsetType.CHARS:
                assert label_off.length == len(label_off.value.decode('utf8')), (label_off.length, repr(label_off.value))
            else:
                ## had better by BYTES
                assert label_off.length == len(label_off.value), (label_off.length, repr(label_off.value))

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

                '''
                Cannot do the check below, because Serif actually *changes* the characters that it puts in Token.token....
                InvalidStreamItem: [(55701, '2002'), (55706, 'coup'), (55711, "d'etat")] not in '2002 coup d\xe2\x80\x99etat'

                if tok.token not in label_off.value and \
                   cleanse(tok.token.decode('utf8')) not in \
                   cleanse(label_off.value.decode('utf8')):
                    raise InvalidStreamItem(
                        '%r not in %r' %
                        ([(t.offsets[offset_type].first, t.token)
                          for t in toks],
                         label_off.value))
                '''


def look_ahead_match(rating, tokens):
    '''iterate through all tokens looking for matches of cleansed tokens
    or token regexes, skipping tokens left empty by cleansing and
    coping with Token objects that produce multiple space-separated
    strings when cleansed.  Yields tokens that match.

    '''
    ## this ensures that all cleansed tokens are non-zero length
    all_mregexes = []
    for m in rating.mentions:
        mregexes = []
        mpatterns = m.decode('utf8').split(' ')
        for mpat in mpatterns:
            if mpat.startswith('ur"^') and mpat.endswith('$"'): # is not regex
                ## chop out the meat of the regex so we can reconstitute it below
                mpat = mpat[4:-2]
            else:
                mpat = cleanse(mpat)
            if mpat:
                ## make a unicode raw string
                ## https://docs.python.org/2/reference/lexical_analysis.html#string-literals
                mpat = ur'^%s$' % mpat
                logger.debug('look_ahead_match compiling regex: %s', mpat)
                mregexes.append(re.compile(mpat, re.UNICODE | re.IGNORECASE))

        if not mregexes:
            logger.warn('got empty cleansed mention: %r\nrating=%r' % (m, rating))

        all_mregexes.append(mregexes)

    ## now that we have all_mregexes, go through all the tokens
    for i in range(len(tokens)):
        for mregexes in all_mregexes:
            if mregexes[0].match(tokens[i][0][0]):
                ## found the start of a possible match, so iterate
                ## through the tuples of cleansed strings for each
                ## Token while stepping through the cleansed strings
                ## for this mention.
                m_j = 1
                i_j = 0
                last_token_matched = 0
                matched = True
                while m_j < len(mregexes):
                    i_j += 1
                    if i_j == len(tokens[i + last_token_matched][0]):
                        i_j = 0
                        last_token_matched += 1
                        if i + last_token_matched == len(tokens):
                            matched = False
                            break
                    target_token = tokens[i + last_token_matched][0][i_j]
                    ## this next line is the actual string comparison
                    if mregexes[m_j].match(target_token):
                        m_j += 1
                    elif target_token == '':
                        continue
                    else:
                        matched = False
                        break
                if matched:
                    ## yield each matched token only once
                    toks = set()
                    for j in xrange(last_token_matched + 1):
                        toks.add(tokens[i + j][1])
                    for tok in toks:
                        yield tok

def multi_token_match(stream_item, aligner_data):
    '''
    iterate through tokens looking for near-exact matches to strings
    in si.ratings...mentions
    '''    
    tagger_id = _get_tagger_id(stream_item, aligner_data)
    sentences = stream_item.body.sentences.get(tagger_id)
    if not sentences:
        return
    ## construct a list of tuples, where the first part of each tuple
    ## is a tuple of cleansed strings, and the second part is the
    ## Token object from which it came.
    tokens = map(lambda tok: (cleanse(tok.token.decode('utf8')).split(' '), tok), 
                 itertools.chain(*[sent.tokens for sent in sentences]))    
    required_annotator_id = aligner_data['annotator_id']
    for annotator_id, ratings in stream_item.ratings.items():
        if (required_annotator_id is None) or (annotator_id == required_annotator_id):
            for rating in ratings:
                label = Label(annotator=rating.annotator,
                              target=rating.target)
                
                num_tokens_matched = 0
                for tok in look_ahead_match(rating, tokens):
                    if aligner_data.get('update_labels'):
                        tok.labels.pop(annotator_id, None)
                    add_annotation(tok, label)
                    num_tokens_matched += 1

                if num_tokens_matched == 0:
                    logger.warning('multi_token_match didn\'t actually match '
                                   'entity %r in stream_id %r',
                                   rating.target.target_id,
                                   stream_item.stream_id)
                else:
                    logger.debug('matched %d tokens for %r in %r',
                                 num_tokens_matched, rating.target.target_id,
                                 stream_item.stream_id)

                ## stream_item passed by reference, so nothing to return


def make_memory_info_msg(clean_visible_path=None, ner_xml_path=None):
    msg = 'reporting memory while running on:\n%r\n%r' % (clean_visible_path, ner_xml_path)
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
    #'line_offset_labels': line_offset_labels,
    'byte_offset_labels': byte_offset_labels,
    'char_offset_labels': char_offset_labels,
    'multi_token_match': multi_token_match,
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

    if aligner_data.get('cleanup_tmp_files', True):
        os.rename(t_path2, t_path1)
    else:
        # for development, leave intermediate tmp file
        shutil.copy(t_path2, t_path1)


class _aligner_batch_transform(streamcorpus_pipeline.stages.BatchTransform, streamcorpus_pipeline.stages.IncrementalTransform):
    # aligner needs to be a single element tuple containing a function pointer,
    # because otherwise a function pointer assigned at class definition scope becomes
    # "bound" to the class and will be called with a suprious first 'self' argument.
    aligner = None

    def process_path(self, chunk_path):
        # implement BatchTransform
        _aligner_core(chunk_path, self.aligner[0], self.config)

    def process_item(self, stream_item, context):
        # implement IncrementalTransform
        aligner = self.aligner[0]
        aligner(stream_item, self.config)
        return stream_item

    def shutdown(self):
        pass


class name_align_labels(_aligner_batch_transform):
    '''
    requires config['chain_selector'] = ALL | ANY
    requires config['annotator_id'] (which person/org did manual labelling)
    '''
    config_name = 'name_align_labels'
    default_config = {
        'chain_selector': 'ALL'
    }
    @staticmethod
    def check_config(config, name):
        if 'chain_selector' not in config:
            raise ConfigurationError('{0} requires chain_selector'.format(name))
        if config['chain_selector'] not in _CHAIN_SELECTORS:
            raise ConfigurationError('chain_selector must be one of: {0!r}'.format(_CHAIN_SELECTORS.keys()))
    aligner = (names_in_chains,)

class line_offset_align_labels(_aligner_batch_transform):
    '''
    requires config['annotator_id'] (which person/org did manual labelling)
    requires config['tagger_id'] (which software did tagging to process)
    '''
    config_name = 'line_offset_align_labels'
    aligner = (line_offset_labels,)

class byte_offset_align_labels(_aligner_batch_transform):
    '''
    requires config['annotator_id'] (which person/org did manual labelling)
    requires config['tagger_id'] (which software did tagging to process)
    '''
    config_name = 'byte_offset_align_labels'
    aligner = (byte_offset_labels,)

class char_offset_align_labels(_aligner_batch_transform):
    '''
    requires config['annotator_id'] (which person/org did manual labelling)
    requires config['tagger_id'] (which software did tagging to process)
    '''
    config_name = 'char_offset_align_labels'
    aligner = (char_offset_labels,)

class multi_token_match_align_labels(_aligner_batch_transform):
    '''
    requires config['annotator_id'] (which person/org did manual labelling)
    requires config['tagger_id'] (which software did tagging to process)
    '''
    config_name = 'multi_token_match_align_labels'
    aligner = (multi_token_match,)


class TaggerBatchTransform(streamcorpus_pipeline.stages.BatchTransform):
    '''
    streamcorpus.pipeline.TaggerBatchTransform provides a structure for
    aligning a taggers output with labels and generating
    stream_item.sentences[tagger_id] = [Sentence]
    '''
    template = None

    def __init__(self, *args, **kwargs):
        super(TaggerBatchTransform, self).__init__(*args, **kwargs)
        self._child = None
        self.config['tagger_root_path'] = \
            os.path.join(self.config['third_dir_path'], 
                         self.config['path_in_third'])

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
%(tagger_root_path)s as the path from the config file,
%(clean_visible_path)s as the input XML file, and %(ner_xml_path)s as
the output path to create.
''')
        tagger_config = dict(
            tagger_root_path=self.config['tagger_root_path'],
            clean_visible_path=clean_visible_path,
            ner_xml_path=ner_xml_path)
        ## get a java_heap_size or default to 1GB
        tagger_config['java_heap_size'] = self.config.get('java_heap_size', '')
        cmd = self.template % tagger_config
        start_time = time.time()
        ## make sure we are using as little memory as possible
        gc.collect()
        try:
            self._child = subprocess.Popen(cmd, stderr=subprocess.PIPE, shell=True)
        except OSError, exc:
            msg = traceback.format_exc(exc)
            msg += make_memory_info_msg(clean_visible_path, ner_xml_path)
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
