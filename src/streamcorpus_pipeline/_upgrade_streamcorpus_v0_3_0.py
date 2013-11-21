'''
This software is released under an MIT/X11 open source license.

Copyright 2012-2013 Diffeo, Inc.
'''
import copy
import logging
import streamcorpus

logger = logging.getLogger(__name__)

content_item_attrs = ['raw', 'encoding', 'media_type', 'clean_html', 'clean_visible', 'logs', 
                      'taggings', 'labels', 'language', 
                      ]

class upgrade_streamcorpus_v0_3_0(object):
    def __init__(self, config):
        self._config = config

    def __call__(self, si, context=None):
        if si.version == streamcorpus.Versions.v0_3_0:
            return si

        if not hasattr(si, 'version'):
            raise NotImplementedError('upgrade_streamcorpus_v0_3_0 does not support upgrading from v0_1_0; see "_upgrade_streamcorpus.py"')

        si3 = streamcorpus.make_stream_item(
            zulu_timestamp=si.stream_time.zulu_timestamp,
            abs_url=si.abs_url)

        if si3.stream_id != si.stream_id:
            si3.external_ids['kba-2013'] = {si3.stream_id: si.stream_id}

        ## copy everything 
        for attr in ['original_url', 'ratings', 'schost', 'source', 'source_metadata',
                     'ratings', ]:
            setattr(si3, attr, copy.deepcopy(getattr(si, attr)))

        si3.body = streamcorpus.ContentItem()

        for name, ci in si.other_content.items():
            ci3 = streamcorpus.ContentItem()
            si3.other_content[name] = ci3
            for attr in content_item_attrs:
                setattr(ci3, attr, copy.deepcopy(getattr(ci, attr)))

        for attr in content_item_attrs:
            setattr(si3.body, attr, copy.deepcopy(getattr(si.body, attr)))

        ## fix the body.sentences['lingpipe'] mention_id ranges
        next_global_mention_id = 0
        ## mapping from (sentence_id, mention_id) --> global_mention_id
        mention_ids = {}
        si3.body.sentences['lingpipe'] = []
        for sentence_id, sentence in enumerate(si.body.sentences.get('lingpipe', [])):
            new_sent = streamcorpus.Sentence()
            si3.body.sentences['lingpipe'].append(new_sent)

            for token_id, token in enumerate(sentence.tokens):

                new_token = streamcorpus.Token()
                new_sent.tokens.append(new_token)

                for attr in ['token_num', 'token', 'offsets', 'sentence_pos', 'lemma', 'pos', 'entity_type', 'mention_id', 'equiv_id', 'parent_id', 'dependency_path', 'labels']:
                    setattr(new_token, attr, copy.deepcopy(getattr(token, attr)))

                if token.mention_id not in [-1, None]:
                    key = (sentence_id, token.mention_id)
                    if key in mention_ids:
                        new_mention_id = mention_ids[key]

                    else:
                        new_mention_id = next_global_mention_id
                        next_global_mention_id += 1

                        ## save it for later
                        mention_ids[key] = new_mention_id

                    new_token.mention_id = new_mention_id
                    logger.debug('new_mention_id = %d' % new_mention_id)

                    if token.entity_type in [3, 4]:
                        ## convert FEMALE/MALE_PRONOUN
                        new_token.mention_type = streamcorpus.MentionType.PRO
                        new_token.entity_type  = streamcorpus.EntityType.PER
                        
                        if token.entity_type == 3:
                            gender_value = 1
                        else:
                            gender_value = 0

                        attr = streamcorpus.Attribute(
                            attribute_type = streamcorpus.AttributeType.PER_AGE,
                            evidence = token.token,
                            value = str(gender_value),
                            sentence_id = sentence_id,
                            mention_id = token.mention_id)

                        if 'lingpipe' not in si3.body.attributes:
                            si3.body.attributes['lingpipe'] = []
                        si3.body.attributes['lingpipe'].append(attr)

                    else:
                        new_token.mention_type = streamcorpus.MentionType.NAME

        ## return our newly manufacturered v0_3_0 StreamItem
        return si3
