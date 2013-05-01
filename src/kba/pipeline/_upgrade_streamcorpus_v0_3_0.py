import copy
import logging
import streamcorpus

logger = logging.getLogger(__name__)

class upgrade_streamcorpus_v0_3_0(object):
    def __init__(self, config):
        self._config = config

    def __call__(self, si):
        if si.version == streamcorpus.Versions.v0_3_0:
            return si

        if not hasattr(si, 'version'):
            raise NotImplementedError('upgrade_streamcorpus_v0_3_0 does not support upgrading from v0_1_0; see "_upgrade_streamcorpus.py"')

        si3 = streamcorpus.make_stream_item(
            zulu_timestamp=si.stream_time.zulu_timestamp,
            abs_url=si.abs_url)

        ## copy everything 
        for attr in ['body', 'original_url', 'other_content', 'ratings', 'schost', 'source', 'source_metadata']:
            setattr(si3, attr, copy.deepcopy(getattr(si, attr)))


        ## fix the body.sentences['lingpipe'] mention_id ranges
        next_global_mention_id = 0
        ## mapping from (sentence_id, mention_id) --> global_mention_id
        mention_ids = {}
        for sentence_id, sentence in enumerate(si3.body.sentences.get('lingpipe', [])):
            for token_id, token in enumerate(sentence.tokens):

                if token.mention_id not in [-1, None]:
                    key = (sentence_id, token.mention_id)
                    if key in mention_ids:
                        new_mention_id = mention_ids[key]

                    else:
                        new_mention_id = next_global_mention_id
                        next_global_mention_id += 1

                        ## save it for later
                        mention_ids[key] = new_mention_id

                    token.mention_id = new_mention_id
                    logger.debug('new_mention_id = %d' % new_mention_id)

        ## return our newly manufacturered v0_3_0 StreamItem
        return si3
