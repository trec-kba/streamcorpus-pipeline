'''
This software is released under an MIT/X11 open source license.

Copyright 2012-2013 Diffeo, Inc.
'''
from __future__ import division
import os
import math
import hashlib
from itertools import ifilter, imap
try:
    from collections import Counter
except ImportError:
    from backport_collections import Counter
from nltk.corpus import stopwords
from _clean_visible import cleanse

def tps(text, min_token_len=2, quant_rate=0.01):
    '''
    :param text: tag-free UTF-8 string of free text
    :returns string: 32-character md5 hash in hexadecimal

    Python implementation of the TextProfileSignature provided in
    SOLR.  Unlike most other locality sensitive hashes, a TPS can be
    indexed as a searchable property of each document that does not
    require n^2 comparisons to find duplicates.

    http://wiki.apache.org/solr/TextProfileSignature
    '''
    
    counts = Counter(
        ifilter(lambda x: len(x) >= min_token_len, 
                imap(cleanse, text.split())))

    max_freq = counts.most_common(1)[0][1]
    if max_freq <= 1:
        quant = 1
    else:
        quant = max(2, round(max_freq * quant_rate))

    to_hash = []
    for word, count in counts.most_common():
        if count <= quant:
            break
        to_hash += [word, str(int(math.floor(count / quant)))]

    to_hash = u' '.join(to_hash)
    return hashlib.md5(to_hash.encode('utf8')).hexdigest()
