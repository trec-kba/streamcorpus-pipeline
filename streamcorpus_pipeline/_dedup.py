'''
Simple dedup tools that rejects StreamItems with a previously seen
doc_id or have similar nilsimsa hashes.

This software is released under an MIT/X11 open source license.

Copyright 2012-2014 Diffeo, Inc.
'''
from __future__ import absolute_import
import logging
import os
import sys

import nilsimsa

from streamcorpus_pipeline.stages import Configured

logger = logging.getLogger(__name__)

class dedup(Configured):
    config_name = 'dedup'
    def __init__(self, *args, **kwargs):
        super(dedup, self).__init__(*args, **kwargs)
        ## keep a mapping from doc_id to nilsimsa hexdigests
        self._doc_ids = dict()
        self._count = 0

    def __call__(self, si, context):
        self._count += 1

        nil = None

        ## get the desired content from the text
        content = getattr(si.body, self.config['content_form'], '')
        if content is None:
            content = ''
        len_raw = len( getattr(si.body, 'raw', '') )

        ## we will always reject if the doc_id is the same and the
        ## sim and len requirements pass
        if si.doc_id in self._doc_ids:

            stream_id2, abs_url2, nil2, content2, len_raw2 = self._doc_ids[si.doc_id]

            if not (content and content2):  ## fall back to using raw
                if max( len_raw, len_raw2 ) == 0:
                    ## reject is the length of everything is zero
                    return None
                len_sim_raw = 1 - float( abs( len_raw - len_raw2 ) ) / max( len_raw, len_raw2 )
                if 1000 * len_sim_raw >= self.config['min_len_sim_thousandths_raw']:
                    logger.info('rejecting same doc %s, no si.body.%s, len_raw_sim_frac=%d >= %d=min_len_sim_thousandths_raw, lang=%r' % (
                            si.stream_id, self.config['content_form'], 1000 * len_sim_raw, self.config['min_len_sim_thousandths_raw'], 
                            si.body.language and si.body.language.code or None))
                    return None
                else:
                    logger.info('keeping doc %s (same page as %s), no si.body.%s, len_raw_sim_frac=%d > %d=min_len_sim_thousandths_raw' % (
                            si.stream_id, stream_id2, self.config['content_form'], 1000 * len_sim_raw, self.config['min_len_sim_thousandths_raw']))


            elif not self.config['use_nilsimsa']:
                len_sim = 1 - float( abs(len(content) - len(content2)) ) / max(len(content), len(content2))
                if 1000 * len_sim >= self.config['min_len_sim_thousandths_clean']:
                    logger.info(
                        'rejecting same doc_id sim=not-computed, len=(%d +/- %d) %s %s %r %r' \
                        % (len(content), len_sim, si.stream_id, stream_id2, si.abs_url, abs_url2))
                    ## reject it!
                    return None


            else:
                ## get the data from the previously seen item with same doc_id
                stream_id2, abs_url2, nil2, content2, len_raw2 = self._doc_ids[si.doc_id]

                ## compute and compare the nilsimsa hashes
                nil = nilsimsa.Nilsimsa(content).hexdigest()
                sim = nilsimsa.compare_digests( nil, nil2 )

                if self.config['exactness_nilsimsa_threshold'] <= sim:
                    if self.config['min_clean_length'] <= len(content):
                        len_sim = 1 - float( abs(len(content) - len(content2)) ) / max(len(content), len(content2))
                        if 1000 * len_sim >= self.config['min_len_sim_thousandths_clean']:
                            logger.info(
                                'rejecting same doc_id sim=%d len=(%d +/- %d) %s %r' \
                                % (sim, len(content), len_sim, si.doc_id, si.abs_url))
                            ## reject it!
                            return None

        if content and not self.config['require_same_doc_id'] and self.config['use_nilsimsa']:
            if not nil:
                nil = nilsimsa.Nilsimsa(content).hexdigest()

            for doc_id, (stream_id2, abs_url2, nil2, content2, len_raw_2) in self._doc_ids.items():
                sim = nilsimsa.compare_digests( nil, nil2)

                if sim >= self.config['log_nilsimsa_threshold']:
                    if 'log_dir_path' in self.config:
                        ## write to disk
                        if not os.path.exists(self.config['log_dir_path']):
                            os.makedirs(self.config['log_dir_path'])
                        first = os.path.join(self.config['log_dir_path'], '%d-%s-%s.html' % (sim, si.doc_id, doc_id))
                        fh = open(first, 'wb')
                        fh.write(content)
                        fh.write('\n\n---NEW DOC --\n\n')
                        fh.write(content2)
                        fh.close()

                if sim >= self.config['exactness_nilsimsa_threshold']:
                    len_sim = abs(len(content) - len(content2))
                    logger.info( 'rejecting sim=%d len=(%d +/- %d) %s %s  %r %r' \
                                     % (sim, len(content), len_sim, doc_id, si.doc_id, abs_url2, si.abs_url) )
                    return None
                else:
                    logger.info( 'observed sim=%d %s %s  %r %r' % (sim, doc_id, si.doc_id, abs_url2, si.abs_url) )

        ## not rejecting, so keep data for further comparisons within this pipeline run
        if self.config['use_nilsimsa']:
            if not nil:
                nil = nilsimsa.Nilsimsa(content).hexdigest()
        else:
            nil = None

        logger.debug('dedup caching %s len(content) = %s, language=%r' % (
                si.stream_id, len(content), 
                si.body.language and si.body.language.code or None))
        self._doc_ids[ si.doc_id ] = (si.stream_id, si.abs_url, nil, content, len_raw)

        return si

