'''Read and write stream items to/from :mod:`kvlayer`.

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2014 Diffeo, Inc.
'''
from __future__ import absolute_import
import ctypes
from collections import Counter
import hashlib
import logging
import string
import struct
import uuid


import kvlayer
import streamcorpus
from streamcorpus_pipeline.stages import Configured
from streamcorpus_pipeline._kvlayer_keyword_search import \
    INDEXING_DEPENDENCIES_FAILED, keyword_indexer
from streamcorpus_pipeline._kvlayer_table_names import \
    WITH_SOURCE, \
    HASH_TF_SID, HASH_KEYWORD, \
    stream_id_to_kvlayer_key, kvlayer_key_to_stream_id, \
    STREAM_ITEM_TABLE_DEFS, INDEX_TABLE_NAMES, \
    STREAM_ITEMS_TABLE, STREAM_ITEMS_SOURCE_INDEX


import yakonfig

logger = logging.getLogger(__name__)


class WrongNumberException(Exception):
    pass

def iter_one_or_ex(gen):
    # for when you want exactly one result out of an iterable.
    # raise WrongNumberException otherwise.
    val = None
    any = False
    for tv in gen:
        if not any:
            any = True
            val = tv
        else:
            raise WrongNumberException("only wanted one, but got another")
    if not any:
        raise WrongNumberException("didn't get any")
    return val


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
        self.client.setup_namespace(STREAM_ITEM_TABLE_DEFS)


    def _loadkey(self, k):
        key, data = iter_one_or_ex(self.client.get(STREAM_ITEMS_TABLE, k))
        errors, data = streamcorpus.decrypt_and_uncompress(data)
        if errors:
            raise errors
        yield streamcorpus.deserialize(data)

    def _loadrange(self, keya, keyb):
        for key, data in self.client.scan(STREAM_ITEMS_TABLE, (keya, keyb)):
            errors, data = streamcorpus.decrypt_and_uncompress(data)
            if errors:
                logger.error('could not decrypet_and_uncompress %s: %s', key, errors)
                continue
            yield streamcorpus.deserialize(data)

    ## TODO: make this able to take a keyword query as input for
    ## processing stream_ids that match a keyword query; will use the
    ## HASH_TF_SID index created by to_kvlayer;
    ## probably also needs a table about the state of completion of
    ## each StreamItem
    def __call__(self, i_str):
        if not i_str:
            for si in self._loadrange(None, None):
                yield si
            return
        for si in parse_keys_and_ranges(i_str, self._loadkey, self._loadrange):
            yield si


def parse_keys_and_ranges(i_str, keyfunc, rangefunc):
    '''
    parse 20 byte key blobs (16 bytes md5 hash, 4 bytes timestamp) split by ';' or '<'
    e.g.
    'a<f;x' loads scans keys a through f and loads singly key x

    keyfunc and rangefunc are run as generators and their yields are yielded from this function.
    '''
    while i_str:
        if len(i_str) == SI_KEY_LENGTH:
            # one key, get it.
            key = parse_si_key(i_str)
            for retval in keyfunc(key):
                yield retval
            return

        keya = i_str[:SI_KEY_LENGTH]
        splitc = i_str[SI_KEY_LENGTH]
        if splitc == '<':
            # range
            keyb = i_str[SI_KEY_LENGTH+1:SI_KEY_LENGTH+1+SI_KEY_LENGTH]
            i_str = i_str[SI_KEY_LENGTH+1+SI_KEY_LENGTH:]
            keya = parse_si_key(keya)
            keyb = parse_si_key(keyb)
            for retval in rangefunc(keya, keyb):
                yield retval
        elif splitc == ';':
            # keya is single key to load
            keya = parse_si_key(keya)
            for retval in keyfunc(keya):
                yield retval
            i_str = i_str[SI_KEY_LENGTH+1+1:]
        else:
            logger.error('bogus key splitter %s, %r', splitc, i_str)
            return
        # continue parsing i_str


def get_kvlayer_stream_item(client, stream_id):
    '''Retrieve a :class:`streamcorpus.StreamItem` from :mod:`kvlayer`.

    This function requires that `client` already be set up properly::

        client = kvlayer.client()
        client.setup_namespace(STREAM_ITEM_TABLE_DEFS)
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
    key = stream_id_to_kvlayer_key(stream_id)
    for k, v in client.get(STREAM_ITEMS_TABLE, key):
        if v is not None:
            errors, bytestr = streamcorpus.decrypt_and_uncompress(v)
            return streamcorpus.deserialize(bytestr)
    raise KeyError(stream_id)


class to_kvlayer(Configured):
    '''Writer that puts stream items in :mod:`kvlayer`.

    stores StreamItems in a kvlayer table in the
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
    default_config = {'indexes': []}

    @staticmethod
    def check_config(config, name):
        yakonfig.check_toplevel_config(kvlayer, name)
        for ndx in config['indexes']:
            if ndx not in INDEX_TABLE_NAMES:
                raise yakonfig.ConfigurationError(
                    'invalid {} indexes type {!r}'
                    .format(name, ndx))

        keyword_indexer.check_config(config, name)

    def __init__(self, *args, **kwargs):
        super(to_kvlayer, self).__init__(*args, **kwargs)
        self.client = kvlayer.client()
        self.client.setup_namespace(STREAM_ITEM_TABLE_DEFS)

        if HASH_KEYWORD in self.config['indexes']:
            self.keyword_indexer = keyword_indexer(self.client)

    def __call__(self, t_path, name_info, i_str):
        si_keys = []
        for si_key in put_stream_items(self.client, streamcorpus.Chunk(t_path), self.config):
            si_keys.append(serialize_si_key(si_key))
        return si_keys


SI_KEY_LENGTH = 20


def serialize_si_key(si_key):
    '''
    Return packed bytes representation of StreamItem kvlayer key.
    The result is 20 bytes, 16 of md5 hash, 4 of int timestamp.
    '''
    assert len(si_key[0]) == 16, 'bad StreamItem key, expected 16 byte md5 hash binary digest, got: {!r}'.format(si_key)
    return struct.pack('>16si', si_key[0], si_key[1])


def parse_si_key(si_key_bytes):
    '''
    Parse result of serialize_si_key()
    Return kvlayer key tuple.
    '''
    return struct.unpack('>16si', si_key_bytes)


def streamitem_to_key_data(si):
    '''
    extract the parts of a StreamItem that go into a kvlayer key, convert StreamItem to blob for storage.

    return (kvlayer key tuple), data blob
    '''
    key = key_for_stream_item(si)
    data = streamcorpus.serialize(si)
    errors, data = streamcorpus.compress_and_encrypt(data)
    assert not errors, errors
    return key, data


def key_for_stream_item(si):
    '''
    Return kvlayer key for a StreamItem
    key is (bytes of hash of abs_url, int(timestamp))
    '''
    # get binary 16 byte digest
    urlhash = hashlib.md5(si.abs_url).digest()
    return (urlhash, int(si.stream_time.epoch_ticks))


KVLAYER_DEFAULT_CONFIG = {
    'indexes': [],
}


def index_source(si):
    '''
    yield (table name, (key, value))
    '''
    if si.source:
        yield (STREAM_ITEMS_SOURCE_INDEX, (key_for_stream_item(si), si.source))


# functions take a StreamItem and yield (table name, (key, value))
INDEX_FUNCTIONS = {
    WITH_SOURCE: index_source,
}


class TableBuffer(object):
    def __init__(self, kvl, table_name, buffer_count=100):
        self.kvl = kvl
        self.table_name = table_name
        self.buffer_count = buffer_count
        self.buffer = []

    def put(self, key, value):
        self.buffer.append( (key, value) )
        if len(self.buffer) >= self.buffer_count:
            self.kvl.put(self.table_name, *self.buffer)
            self.buffer = []

    def flush(self):
        self.kvl.put(self.table_name, *self.buffer)
        self.buffer = []


def put_stream_items(kvl, itemiter, config=None):
    '''
    Puts StreamItem objects from itemiter.
    Yields kvlayer key tuples.
    '''
    if config is None:
        config = KVLAYER_DEFAULT_CONFIG
    sitable = TableBuffer(kvl, STREAM_ITEMS_TABLE)
    outputs = {}
    indexes = config.get('indexes', [])
    for index_name in indexes:
        itn = INDEX_TABLE_NAMES[index_name]
        outputs[itn] = TableBuffer(kvl, itn)

    for si in itemiter:
        si_key = key_for_stream_item(si)
        data = streamcorpus.serialize(si)
        errors, data = streamcorpus.compress_and_encrypt(data)
        assert not errors, errors
        sitable.put(si_key, data)
        yield si_key

        for index_name in indexes:
            index_func = INDEX_FUNCTIONS[index_name]
            for tablename, kv in index_func(si):
                outputs[tablename].put(*kv)

    sitable.flush()
    for outbuf in outputs.itervalues():
        outbuf.flush()
