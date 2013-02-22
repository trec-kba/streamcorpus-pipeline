'''
Simple dedup tools that rejects StreamItems with a previously seen
doc_id or have similar nilsimsa hashes.

'''
import os
import logging
import nilsimsa

logger = logging.getLogger(__name__)

class dedup(object):
    def __init__(self, config):
        self.config = config
        ## keep a mapping from doc_id to nilsimsa hexdigests
        self._doc_ids = dict()
        self._count = 0

    def __call__(self, si):
        self._count += 1

        nil = None

        ## get the desired content from the text
        content = getattr(si.body, self.config['content_form'], None)

        ## we will always reject if the doc_id is the same and the
        ## sim and len requirements pass
        if si.doc_id in self._doc_ids:

            if not content:
                logger.critical('duplicate doc %s has not si.body.%s' % (
                        si.doc_id, self.config['content_form']))

            else:
                ## get the data from the previously seen item with same doc_id
                abs_url2, nil2, content2 = self._doc_ids[si.doc_id]

                ## compute and compare the nilsimsa hashes
                nil = nilsimsa.Nilsimsa(content).hexdigest()
                sim = nilsimsa.compare_hexdigests( nil, nil2 )

                if self.config['exactness_nilsimsa_threshold'] <= sim:
                    if self.config['min_doc_length'] <= len(content):
                        len_diff = abs(len(content) - len(content2))
                        if len_diff <= self.config['max_doc_length_difference']:
                            logger.critical(
                                'rejecting same doc_id sim=%d len=(%d +/- %d) %s %r' \
                                % (sim, len(content), len_diff, si.doc_id, si.abs_url))
                            ## reject it!
                            return None

                if 'log_dir_path' in self.config:
                    if not os.path.exists(self.config['log_dir_path']):
                        os.makedirs(self.config['log_dir_path'])

                    first = os.path.join(self.config['log_dir_path'], '0-%s.html' % si.doc_id)
                    if not os.path.exists(first):
                        ## no need to overwrite it
                        fh = open(first, 'wb')
                        fh.write('href: %r\n\n' % abs_url2)
                        fh.write(content2)
                        fh.close()

                    next = os.path.join(self.config['log_dir_path'], '%d_%r_%s.html' % (self._count, sim, si.doc_id))
                    fh = open(next, 'wb')
                    fh.write('href: %r\n\n' % si.abs_url)
                    fh.write(content)
                    fh.close()

        if content and not self.config['require_same_doc_id']:
            if not nil:
                nil = nilsimsa.Nilsimsa(content).hexdigest()

            for doc_id, (abs_url2, nil2, content2) in self._doc_ids.items():
                sim = nilsimsa.compare_hexdigests( nil, nil2)

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
                    len_diff = abs(len(content) - len(content2))
                    logger.info( 'rejecting sim=%d len=(%d +/- %d) %s %s  %r %r' \
                                     % (sim, len(content), len_diff, doc_id, si.doc_id, abs_url2, si.abs_url) )
                    return None
                else:
                    logger.info( 'observed sim=%d %s %s  %r %r' % (sim, doc_id, si.doc_id, abs_url2, si.abs_url) )

        ## not rejecting, so keep data for further comparisons within this pipeline run
        if not nil:
            nil = nilsimsa.Nilsimsa(content).hexdigest()
        self._doc_ids[ si.doc_id ] = (si.abs_url, nil, content)

        return si

