'''
A streamcorpus_pipeline stage that detects when a StreamItem
contains an exact or fuzzy match to a name in a list of names

This software is released under an MIT/X11 open source license.

Copyright 2012-2013 Diffeo, Inc.

'''
import re
import os
import sys
import time
import string
import traceback
import jellyfish
from itertools import islice, chain

def log(mesg):
    sys.stderr.write("%s\n" % mesg)
    sys.stderr.flush()

class exact_matcher(object):
    def __init__(self, config):
        self.config = config
        ## load trie from flat file
        self.trie = None

    def __call__(self, si):
        
        if not si.body and si.body.sentences:
            print('name_matcher: found no sentences for %r' % si.stream_id)
            return si




'''

parts from old jarowinkler matcher

        ## sweep windows across tokens, ignoring sentence boundaries.
        for win in window(chain(*[lambda sent: sent.tokens 
                                  for sent in si.body.sentences[self.config['tagger_id']]])):
            print win

    entity_representations = prepare_entities(entities)

            ## construct a scorer, which means make all the bigrams and their codexes
            scorer = Scorer(cleansed)

            tasks = {}
            for er in entity_representations:
                score, mention_string = scorer.compute_relevance(er)
                if score > 900:

                    for urlname in entity_representations[er]:
                        #print urlname
                        if urlname not in tasks or tasks[urlname]["jaro_winkler"] < score:
                            tasks[urlname] = make_task(doc, mentions, urlname, cleansed, score)


def window(seq, n=2):
    "Returns a sliding window (of width n) over data from the iterable"
    "   s -> (s0,s1,...s[n-1]), (s1,s2,...,sn), ...                   "
    it = iter(seq)
    result = tuple(islice(it, n))
    if len(result) == n:
        yield result    
    for elem in it:
        result = result[1:] + (elem,)
        yield result


## make a unicode translation table to converts all punctuation to white space
strip_punctuation = dict((ord(char), u" ") for char in string.punctuation)

white_space_re = re.compile("\s+")

def strip_string(s):
    """
    strips punctuation and repeated whitespace from unicode strings
    """
    return white_space_re.sub(" ", s.translate(strip_punctuation).lower())

def prepare_entities(entities):
    """
    Creates a dict keyed on entity name with the values set to a
    representation that is effiicent for the scorer
    """
    prep = {}
    
    for urlname in entities:
        ## create set of tokens from entity's name
        base_name = re.sub("\(.*?\)", "", urlname)
        parts = base_name.split('_')
        names = [re.sub('_', ' ', base_name)]
        if len(names) == 3 and len(names[1]) > 2:
            names.append('%s %s' % (names[0], names[2]))
            names.append('%s %s' % (names[1], names[2]))
            names.append('%s %s' % (names[0], names[1]))

        for n in names:
            if n not in prep:
                prep[n] = []
            ## could have several target entities with the same name:
            prep[n].append(urlname)

    return prep

class Scorer:
    def __init__(self, text):
        """
        Takes text (unicode) and prepare to evaluate entity mentions
        """
        try:
            self.text = text
            toks = self.text.split()

            self.names = []
            for i in range(len(toks) - 1):
                name = '%s %s' % (toks[i], toks[i+1])
                ## parentheses will screw up regexes below, so flatten them now
                name = re.sub('[()]', ' ', name)
                self.names.append(name)
                #print name
            
            self.ready= True

        except Exception, exc:
            ## ignore failures, such as PDFs
            #sys.exit(traceback.format_exc(exc))
            print traceback.format_exc(exc)
            #print repr(text)
            sys.stderr.write("failed to initialize on doc: %s\n" % exc)
            self.ready = False

    def compute_relevance(self, entity_representation):
        """
        Searches text for parts of entity_name

        returns score between zero and 1000, which is intended to be a
        float in [0,1] measured in thousandths.
        """
        ## look for name parts in text:
        #print entity_representation
        scores = []
        best_score = 0
        best_name = None
        for name in self.names:
            if not name.strip():
                continue
            #print "comparing: %s -- %s" % (entity_representation, name.encode('utf8'))
            try:
                score = jellyfish.jaro_distance(entity_representation, name)
            except Exception, exc:
                log('failed on jellyfish.jaro_distance(%r, %r): %s' % (
                        entity_representation, name, str(exc)))
                continue
            if score > best_score:
                best_score = score
                best_name = name
        return int(best_score * 1000), best_name
'''
