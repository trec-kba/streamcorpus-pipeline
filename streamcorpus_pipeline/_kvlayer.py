'''
Provides classes for loading StreamItem objects to/from kvlayer

This software is released under an MIT/X11 open source license.

Copyright 2012-2014 Diffeo, Inc.
'''
from __future__ import absolute_import
import ctypes
import logging
import string
import uuid

import kvlayer
import streamcorpus
from streamcorpus_pipeline.stages import Configured
import yakonfig

logger = logging.getLogger(__name__)

def epoch_ticks_to_uuid(ticks):
    '''Convert seconds since the Unix epoch to a UUID type.

    The actual value is seconds since the Unix epoch, expressed as a
    64-bit signed int, left-padded with 64 zero bits.

    >>> epoch_ticks_to_uuid(946730040)
    UUID('00000000-0000-0000-0000-0000386df438')
    >>> epoch_ticks_to_uuid(-14182920)
    UUID('00000000-0000-0000-ffff-ffffff2795f8')

    '''
    return uuid.UUID(int=ctypes.c_ulonglong(int(ticks)).value)

def uuid_to_epoch_ticks(u):
    '''Convert a UUID type to seconds since the Unix epoch.

    This undoes :func:`epoch_ticks_to_uuid`.

    '''
    return ctypes.c_longlong(u.int).value

class from_kvlayer(Configured):
    '''
    loads StreamItems from a kvlayer table based an i_str passed to
    __call__, where the i_str must be a four-tuple joined on commas
    that provides (epoch_ticks_1, doc_id_1, epoch_ticks_2, doc_id_2)

    If no values are specified, it defaults to the entire table.  If
    one of the epoch_ticks is provided, then both must be provided.
    If one of the doc_ids is provided, then both must be provided.
    That is, half-open ranges are not supported.
    '''
    config_name = 'from_kvlayer'
    @staticmethod
    def check_config(config, name):
        yakonfig.check_toplevel_config(kvlayer, name)

    def __init__(self, *args, **kwargs):
        super(from_kvlayer, self).__init__(*args, **kwargs)
        self.client = kvlayer.client()
        self.client.setup_namespace(
            dict(stream_items=2))

    def __call__(self, i_str):
        if i_str:
            epoch_ticks_1, doc_id_1, epoch_ticks_2, doc_id_2 = i_str.split(',')
            epoch_ticks_1 = epoch_ticks_to_uuid(epoch_ticks_1)
            epoch_ticks_2 = epoch_ticks_to_uuid(epoch_ticks_2)
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

def get_kvlayer_stream_item(client, stream_id):
    '''Retrieve a :class:`streamcorpus.StreamItem` from :mod:`kvlayer`.

    This function requires that `client` already be set up properly::

        client = kvlayer.client()
        client.setup_namespace({'stream_items': 2})
        si = get_kvlayer_stream_item(client, stream_id)

    `stream_id` is in the form of
    :data:`streamcorpus.StreamItem.stream_id` and contains the
    ``epoch_ticks``, a hyphen, and the ``doc_id``.

    :param client: kvlayer client object
    :type client: :class:`kvlayer.AbstractStorage`
    :param str stream_id: stream Id to retrieve
    :return: corresponding :class:`streamcorpus.StreamItem`
    :raise exceptions.KeyError: if `stream_id` is malformed or does
      not correspond to anything in the database

    '''
    # Reminder: stream_id is 1234567890-123456789abcdef...0
    # where the first part is the (decimal) epoch_ticks and the second
    # part is the (hex) doc_id
    parts = stream_id.split('-')
    if len(parts) != 2:
        raise KeyError('invalid stream_id ' + stream_id)
    timestr = parts[0]
    dochex = parts[1]
    if not timestr.isdigit():
        raise KeyError('invalid stream_id ' + stream_id)
    if dochex.lstrip(string.hexdigits) != '':
        raise KeyError('invalid stream_id ' + stream_id)

    key = (uuid.UUID(int=int(timestr)), uuid.UUID(hex=dochex))
    for k,v in client.get('stream_items', key):
        if v is not None:
            errors, bytestr = streamcorpus.decrypt_and_uncompress(v)
            return streamcorpus.deserialize(bytestr)
    raise KeyError(stream_id)

class to_kvlayer(Configured):

    '''
    stores StreamItems in a kvlayer table called "stream_items" in the
    namespace specified in the config dict for this stage.

    each StreamItem is serialized and xz compressed and stored on the
    key (UUID(int=epoch_ticks), UUID(hex=doc_id))

    This key structure supports time-range queries directly on the
    stream_items table.

    The 'indexes' configuration parameter adds additional indexes
    on the data.  Supported index types include:

    ``doc_id_epoch_ticks`` adds an index with the two key parts reversed
    and with no data, allowing range searching by document ID

    ``with_source`` adds an index with the same two key parts as normal,
    but stores the StreamItem's "source" field in the value, allowing
    time filtering on source without decoding documents

    In all cases the index table is `stream_items_` followed by the
    index name.
    '''
    config_name = 'to_kvlayer'
    default_config = { 'indexes': [ 'doc_id_epoch_ticks' ] }
    @staticmethod
    def check_config(config, name):
        yakonfig.check_toplevel_config(kvlayer, name)
        for ndx in config['indexes']:
            if ndx not in to_kvlayer.index_sizes:
                raise yakonfig.ConfigurationError(
                    'invalid {} indexes type {!r}'
                    .format(name, ndx))

    index_sizes = {
        'doc_id_epoch_ticks': 2,
        'with_source': 2,
    }

    def __init__(self, *args, **kwargs):
        super(to_kvlayer, self).__init__(*args, **kwargs)
        self.client = kvlayer.client()
        tables = { 'stream_items': 2 }
        for ndx in self.config['indexes']:
            tables['stream_items_' + ndx] = self.index_sizes[ndx]
        self.client.setup_namespace(tables)

    def __call__(self, t_path, name_info, i_str):
        indexes = {}
        for ndx in self.config['indexes']: indexes[ndx] = []
        # The idea here is that the actual documents can be pretty big,
        # so we don't want to keep them in memory, but the indexes
        # will just be UUID tuples.  So the iterator produces
        def keys_and_values():
            for si in streamcorpus.Chunk(t_path):
                key1 = epoch_ticks_to_uuid(si.stream_time.epoch_ticks)
                key2 = uuid.UUID(hex=si.doc_id)
                data = streamcorpus.serialize(si)
                errors, data = streamcorpus.compress_and_encrypt(data)
                assert not errors, errors

                yield (key1, key2), data

                for ndx in indexes:
                    if ndx == 'doc_id_epoch_ticks':
                        kvp = ((key2, key1), r'')
                    elif ndx == 'with_source':
                        ## si.source can be None but we can't write None blobs to kvlayer
                        if si.source:
                            kvp = ((key1, key2), si.source)
                        else:
                            continue
                    else:
                        assert False, ('invalid index type ' + ndx)
                    indexes[ndx].append(kvp)

        self.client.put( 'stream_items', *list(keys_and_values()))
        for ndx, kvps in indexes.iteritems():
            self.client.put('stream_items_' + ndx, *kvps)
