'''
Provides classes for loading StreamItem objects to/from kvlayer

This software is released under an MIT/X11 open source license.

Copyright 2012-2013 Diffeo, Inc.
'''
import os
import sys
import uuid
import kvlayer
import logging
import streamcorpus

logger = logging.getLogger(__name__)

class from_kvlayer(object):
    def __init__(self, config):
        self.config = config
        self.client = kvlayer.client(config)
        self.client.setup_namespace(
            dict(stream_items=2))

    def __call__(self, i_str):
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

        for key, data in self.client.get( 'stream_items', (key1, key2) ):
            errors, data = streamcorpus.decrypt_and_uncompress(data)
            yield streamcorpus.deserialize(data)


class to_kvlayer(object):
    def __init__(self, config):
        self.config = config
        self.client = kvlayer.client(config)
        self.client.setup_namespace(
            dict(stream_items=2))

    def __call__(self, t_path, name_info, i_str):
        def _keys_and_values():
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

        self.client.put( 'stream_items', *_keys_and_values())
