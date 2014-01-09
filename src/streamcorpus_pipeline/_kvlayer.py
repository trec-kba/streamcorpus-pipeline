'''
Provides classes for loading StreamItem objects to/from kvlayer

This software is released under an MIT/X11 open source license.

Copyright 2012-2013 Diffeo, Inc.
'''
from collections import deque
import os
import sys
import uuid
import kvlayer
import logging
import streamcorpus

logger = logging.getLogger(__name__)

class from_kvlayer(object):
    '''
    loads StreamItems from a kvlayer table based an i_str passed to
    __call__, where the i_str must be a four-tuple joined on commas
    that provides (epoch_ticks_1, doc_id_1, epoch_ticks_2, doc_id_2)  

    If no values are specified, it defaults to the entire table.  If
    one of the epoch_ticks is provided, then both must be provided.
    If one of the doc_ids is provided, then both must be provided.
    That is, half-open ranges are not supported.
    '''
    def __init__(self, config):
        self.config = config
        self.client = kvlayer.client(config)
        self.client.setup_namespace(
            dict(stream_items=2))

    def __call__(self, i_str):
        if i_str:
            epoch_ticks_1, doc_id_1, epoch_ticks_2, doc_id_2 = i_str.split(',')
            epoch_ticks_1 = uuid.UUID(int=int(epoch_ticks_1))
            epoch_ticks_2 = uuid.UUID(int=int(epoch_ticks_2))
            if doc_id_1:
                assert doc_id_2, (doc_id_1, doc_id_2)
                doc_id_1 = uuid.UUID(hex=doc_id_1)
                doc_id_2 = uuid.UUID(hex=doc_id_2)
                key1 = (epoch_ticks_1, doc_id_1)
                key2 = (epoch_ticks_2, doc_id_2)
            else:
                key1 = (epoch_ticks_1, )
                key2 = (epoch_ticks_2, )
            key_ranges = [(key1, key2)]
        else:
            key_ranges = []

        for key, data in self.client.scan( 'stream_items', *key_ranges ):
            errors, data = streamcorpus.decrypt_and_uncompress(data)
            yield streamcorpus.deserialize(data)


class to_kvlayer(object):
    '''
    stores StreamItems in a kvlayer table called "stream_items" in the
    namespace specified in the config dict for this stage.
    
    each StreamItem is serialized and xz compressed and stored on the
    key (UUID(int=epoch_ticks), UUID(hex=doc_id))

    This key structure supports time-range queries directly on the
    stream_items table.

    To search by doc_id, one can look up all epoch_ticks for a given
    doc_id using the index table stream_items_doc_id_epoch_ticks,
    which has the keys reversed and no data.
    '''
    def __init__(self, config):
        self.config = config
        self.client = kvlayer.client(config)
        self.client.setup_namespace(
            dict(stream_items=2,
                 stream_items_doc_id_epoch_ticks=2))

    def __call__(self, t_path, name_info, i_str):
        inverted_keys = deque()
        def keys_and_values():
            total_mb = 0.
            for si in streamcorpus.Chunk(t_path):
                key1 = uuid.UUID(int=si.stream_time.epoch_ticks)
                key2 = uuid.UUID(hex=si.doc_id)
                data = streamcorpus.serialize(si)
                errors, data = streamcorpus.compress_and_encrypt(data)
                assert not errors, errors
                total_mb += float(len(data)) / 2**20
                logger.info('%r, %r --> %d, %.3f', key1, key2, len(data), total_mb)
                yield (key1, key2), data
                inverted_keys.append( ((key2, key1), r'') )

        self.client.put( 'stream_items', *keys_and_values())

        self.client.put( 'stream_items_doc_id_epoch_ticks', *inverted_keys)
