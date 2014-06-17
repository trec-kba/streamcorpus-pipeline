'''Basic example of an external stage for filtering a corpus using
string match; see http://github.com/trec-kba/streamcorpus-filter for a
more sophisticated approach to this basic concept.

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2014 Diffeo, Inc.

Example YAML configuration file streamcorpus-2014-v0_3_0-exact-match-example.yaml
'''
## use the newer regex engine
import regex as re
import string

## make a unicode translation table to converts all punctuation to white space
strip_punctuation = dict((ord(char), u" ") for char in string.punctuation)

white_space_re = re.compile("\s+")

def strip_string(s):
    'strips punctuation and repeated whitespace from unicode strings'
    return white_space_re.sub(" ", s.translate(strip_punctuation).lower())


class filter_exact_match(object):
    'trivial string matcher using simple normalization and regex'

    config_name = 'filter_exact_match'

    def __init__(self, config):
        path = config.get('match_strings_path')
        match_strings = open(path).read().splitlines()
        match_strings = map(strip_string, map(unicode, match_strings))
        self.matcher = re.compile('(.|\n)*?(%s)' % '|'.join(match_strings), re.I)

    def __call__(self, si, context):
        'only pass StreamItems that match'
        if self.matcher.match(strip_string(si.body.clean_visible.decode('utf-8'))):
            return si


## this is how streamcorpus_pipeline finds the stage
Stages = {'filter_exact_match': filter_exact_match}
