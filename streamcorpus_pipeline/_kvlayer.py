'''Read and write stream items to/from :mod:`kvlayer`.

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2015 Diffeo, Inc.

.. autoclass:: from_kvlayer
.. autoclass:: to_kvlayer

'''
from __future__ import absolute_import
import base64
import logging
import re
import struct

import kvlayer
import streamcorpus
from streamcorpus_pipeline.stages import Configured
from streamcorpus_pipeline._kvlayer_table_names import \
    BY_TIME, WITH_SOURCE, KEYWORDS, HASH_TF_SID, HASH_FREQUENCY, \
    HASH_KEYWORD, \
    stream_id_to_kvlayer_key, kvlayer_key_to_stream_id, key_for_stream_item, \
    STREAM_ITEM_TABLE_DEFS, STREAM_ITEM_VALUE_DEFS, INDEX_TABLE_NAMES, \
    STREAM_ITEMS_TABLE, STREAM_ITEMS_SOURCE_INDEX, STREAM_ITEMS_TIME_INDEX
import yakonfig

try:
    from ._kvlayer_keyword_search import keyword_indexer
except ImportError, exc:
    keyword_indexer = str(exc)

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
    '''Read documents from a :mod:`kvlayer` supported database.

    .. currentmodule:: streamcorpus_pipeline._kvlayer

    :mod:`kvlayer` must be included in the top-level configuration.
    Typically this reads stream items previously written by
    :class:`to_kvlayer`.

    The input string is a sequence of stream IDs; see :meth:`__call__`
    for details.  If no values are specified, scans the entire
    :data:`~streamcorpus_pipeline._kvlayer_table_names.STREAM_ITEMS_TABLE`
    table.

    .. automethod:: __call__

    '''
    config_name = 'from_kvlayer'

    @staticmethod
    def check_config(config, name):
        yakonfig.check_toplevel_config(kvlayer, name)

    def __init__(self, *args, **kwargs):
        super(from_kvlayer, self).__init__(*args, **kwargs)
        self.client = kvlayer.client()
        self.client.setup_namespace(STREAM_ITEM_TABLE_DEFS,
                                    STREAM_ITEM_VALUE_DEFS)

    def _loadkey(self, k):
        key, data = iter_one_or_ex(self.client.get(STREAM_ITEMS_TABLE, k))
        errors, data = streamcorpus.decrypt_and_uncompress(data)
        if errors:
            if not isinstance(errors, Exception):
                errors = Exception(repr(errors))
            raise errors
        yield streamcorpus.deserialize(data)

    def _loadrange(self, keya, keyb):
        for key, data in self.client.scan(STREAM_ITEMS_TABLE, (keya, keyb)):
            errors, data = streamcorpus.decrypt_and_uncompress(data)
            if errors:
                logger.error('could not decrypet_and_uncompress %s: %s',
                             key, errors)
                continue
            yield streamcorpus.deserialize(data)

    def __call__(self, i_str):
        '''Actually scan the table, yielding stream items.

        `i_str` consists of a sequence of 20-byte encoded stream IDs.
        These are the 16 bytes of the document ID, plus the 4-byte
        big-endian stream time.  :func:`serialize_si` can produce
        these.  `i_str` can then consist of a single encoded stream
        ID; an encoded stream ID, a literal ``<``, and a second
        encoded stream ID to specify a range of documents; or a list
        of either of the preceding separated by literal ``;``.

        .. todo:: make this support keyword index strings

        '''
        if not i_str:
            for si in self._loadrange(None, None):
                yield si
            return
        for si in parse_keys_and_ranges(i_str, self._loadkey,
                                        self._loadrange):
            yield si


_STREAM_ID_RE = re.compile(r'[0-9]+-[0-9a-fA-F]{32}')


def parse_keys_and_ranges(i_str, keyfunc, rangefunc):
    '''Parse the :class:`from_kvlayer` input string.

    This accepts two formats.  In the textual format, it accepts any
    number of stream IDs in timestamp-docid format, separated by ``,``
    or ``;``, and processes those as individual stream IDs.  In the
    binary format, it accepts 20-byte key blobs (16 bytes md5 hash, 4
    bytes timestamp) split by ``;`` or ``<``; e.g., ``a<f;x`` loads
    scans keys `a` through `f` and loads singly key `x`.

    `keyfunc` and `rangefunc` are run as generators and their yields
    are yielded from this function.

    '''
    while i_str:
        m = _STREAM_ID_RE.match(i_str)
        if m:
            # old style text stream_id
            for retval in keyfunc(stream_id_to_kvlayer_key(m.group())):
                yield retval
            i_str = i_str[m.end():]
            while i_str and ((i_str[0] == ',') or (i_str[0] == ';')):
                i_str = i_str[1:]
            continue

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
        client.setup_namespace(STREAM_ITEM_TABLE_DEFS,
                               STREAM_ITEM_VALUE_DEFS)
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
    if client is None:
        client = kvlayer.client()
        client.setup_namespace(STREAM_ITEM_TABLE_DEFS,
                               STREAM_ITEM_VALUE_DEFS)
    key = stream_id_to_kvlayer_key(stream_id)
    for k, v in client.get(STREAM_ITEMS_TABLE, key):
        if v is not None:
            errors, bytestr = streamcorpus.decrypt_and_uncompress(v)
            return streamcorpus.deserialize(bytestr)
    raise KeyError(stream_id)


def get_kvlayer_stream_item_by_doc_id(client, doc_id):
    '''Retrieve :class:`streamcorpus.StreamItem`s from :mod:`kvlayer`.

    Namely, it returns an iterator over all documents with the given
    docid. The docid should be an md5 hash of the document's abs_url.

    :param client: kvlayer client object
    :type client: :class:`kvlayer.AbstractStorage`
    :param str doc_id: doc id of documents to retrieve
    :return: generator of :class:`streamcorpus.StreamItem`
    '''
    if client is None:
        client = kvlayer.client()
        client.setup_namespace(STREAM_ITEM_TABLE_DEFS,
                               STREAM_ITEM_VALUE_DEFS)
    start = (base64.b16decode(doc_id.upper()), 0)
    end = (base64.b16decode(doc_id.upper()), 0xff)
    for k, v in client.scan(STREAM_ITEMS_TABLE, (start, end)):
        if v is not None:
            errors, bytestr = streamcorpus.decrypt_and_uncompress(v)
            yield streamcorpus.deserialize(bytestr)


def get_kvlayer_stream_ids_by_doc_id(client, doc_id):
    '''Retrieve stream ids from :mod:`kvlayer`.

    Namely, it returns an iterator over all stream ids with the given
    docid. The docid should be an md5 hash of the document's abs_url.

    :param client: kvlayer client object
    :type client: :class:`kvlayer.AbstractStorage`
    :param str doc_id: doc id of documents to retrieve
    :return: generator of str
    '''
    if client is None:
        client = kvlayer.client()
        client.setup_namespace(STREAM_ITEM_TABLE_DEFS,
                               STREAM_ITEM_VALUE_DEFS)
    start = (base64.b16decode(doc_id.upper()), 0)
    end = (base64.b16decode(doc_id.upper()), 0xff)
    for k in client.scan_keys(STREAM_ITEMS_TABLE, (start, end)):
        yield kvlayer_key_to_stream_id(k)


def delete_kvlayer_stream_item(client, stream_id):
    if client is None:
        client = kvlayer.client()
        client.setup_namespace(STREAM_ITEM_TABLE_DEFS,
                               STREAM_ITEM_VALUE_DEFS)
    key = stream_id_to_kvlayer_key(stream_id)
    client.delete(STREAM_ITEMS_TABLE, key)


class to_kvlayer(Configured):
    '''Writer that puts stream items in :mod:`kvlayer`.

    :mod:`kvlayer` must be included in the top-level configuration.
    Compressed stream items are written to the
    :data:`~streamcorpus_pipeline._kvlayer_table_names.STREAM_ITEMS_TABLE`
    table.

    This writer has one configuration parameter: `indexes` adds
    additional indexes on the data.  Supported index types include
    :data:`~streamcorpus_pipeline._kvlayer_table_names.WITH_SOURCE`,
    :data:`~streamcorpus_pipeline._kvlayer_table_names.BY_TIME`,
    :data:`~streamcorpus_pipeline._kvlayer_table_names.HASH_TF_SID`,
    :data:`~streamcorpus_pipeline._kvlayer_table_names.HASH_FREQUENCIES`,
    and
    :data:`~streamcorpus_pipeline._kvlayer_table_names.HASH_KEYWORD`.
    The special index type
    :data:`~streamcorpus_pipeline._kvlayer_table_names.KEYWORDS`
    expands to all of the keyword-index tables.

    If keyword indexing is enabled, it indexes the tokens generated
    by some or all of the taggers that have been run, less a set of
    stop words.  The configuration parameter `keyword_tagger_ids` may
    be set to a list of tagger IDs; if it is set, only the tokens
    from those tagger IDs will be indexed.  If it is set to ``null``
    or left unset, all taggers' tokens will be indexed as distinct
    words.  Any tokens whose size is greater than `keyword_size_limit`
    (default 128) will not be indexed.

    '''
    config_name = 'to_kvlayer'
    default_config = {'indexes': [],
                      'keyword_size_limit': 128}

    @staticmethod
    def check_config(config, name):
        yakonfig.check_toplevel_config(kvlayer, name)
        for ndx in config['indexes']:
            if ndx != KEYWORDS and ndx not in INDEX_TABLE_NAMES:
                raise yakonfig.ConfigurationError(
                    'invalid {0} indexes type {1!r}'
                    .format(name, ndx))
            if isinstance(keyword_indexer, basestring):
                if ndx in [KEYWORDS, HASH_TF_SID, HASH_FREQUENCY,
                           HASH_KEYWORD]:
                    raise yakonfig.ConfigurationError(
                        'cannot configure {0} index {1}: {2}'
                        .format(name, ndx, keyword_indexer))

    @staticmethod
    def normalize_config(config):
        if KEYWORDS in config['indexes']:
            config['indexes'].remove(KEYWORDS)
            config['indexes'].append(HASH_TF_SID)
            config['indexes'].append(HASH_FREQUENCY)
            config['indexes'].append(HASH_KEYWORD)

    def __init__(self, *args, **kwargs):
        super(to_kvlayer, self).__init__(*args, **kwargs)
        self.client = kvlayer.client()
        self.client.setup_namespace(STREAM_ITEM_TABLE_DEFS,
                                    STREAM_ITEM_VALUE_DEFS)

        self.keyword_indexer = None
        hash_docs = HASH_TF_SID in self.config['indexes']
        hash_frequencies = HASH_FREQUENCY in self.config['indexes']
        hash_keywords = HASH_KEYWORD in self.config['indexes']
        keyword_tagger_ids = self.config.get('keyword_tagger_ids', None)
        keyword_size_limit = self.config['keyword_size_limit']
        if hash_docs or hash_frequencies or hash_keywords:
            self.keyword_indexer = keyword_indexer(
                self.client, hash_docs=hash_docs,
                hash_frequencies=hash_frequencies,
                hash_keywords=hash_keywords,
                keyword_tagger_ids=keyword_tagger_ids,
                keyword_size_limit=keyword_size_limit)

    def __call__(self, t_path, name_info, i_str):
        si_keys = []
        sitable = TableBuffer(self.client, STREAM_ITEMS_TABLE)
        outputs = {}
        indexes = self.config.get('indexes', [])
        for index_name in indexes:
            itn = INDEX_TABLE_NAMES.get(index_name)
            if itn is not None:
                outputs[itn] = TableBuffer(self.client, itn)

        for si in streamcorpus.Chunk(t_path):
            si_key = key_for_stream_item(si)
            data = streamcorpus.serialize(si)
            errors, data = streamcorpus.compress_and_encrypt(data)
            assert not errors, errors
            sitable.put(si_key, data)
            si_keys.append(serialize_si_key(si_key))

            for index_name in indexes:
                index_func = INDEX_FUNCTIONS.get(index_name)
                if index_func is not None:
                    for tablename, kv in index_func(si):
                        outputs[tablename].put(*kv)

            if self.keyword_indexer:
                self.keyword_indexer.index(si)

        sitable.flush()
        for outbuf in outputs.itervalues():
            outbuf.flush()

        return si_keys


SI_KEY_LENGTH = 20


def serialize_si_key(si_key):
    '''
    Return packed bytes representation of StreamItem kvlayer key.
    The result is 20 bytes, 16 of md5 hash, 4 of int timestamp.
    '''
    if len(si_key[0]) != 16:
        raise ValueError('bad StreamItem key, expected 16 byte '
                         'md5 hash binary digest, got: {0!r}'.format(si_key))
    return struct.pack('>16si', si_key[0], si_key[1])


def parse_si_key(si_key_bytes):
    '''
    Parse result of serialize_si_key()
    Return kvlayer key tuple.
    '''
    return struct.unpack('>16si', si_key_bytes)


def streamitem_to_key_data(si):
    '''
    extract the parts of a StreamItem that go into a kvlayer key,
    convert StreamItem to blob for storage.

    return (kvlayer key tuple), data blob
    '''
    key = key_for_stream_item(si)
    data = streamcorpus.serialize(si)
    errors, data = streamcorpus.compress_and_encrypt(data)
    assert not errors, errors
    return key, data


def index_source(si):
    '''
    yield (table name, (key, value))
    '''
    if si.source:
        yield (STREAM_ITEMS_SOURCE_INDEX, (key_for_stream_item(si), si.source))


def index_time(si):
    key = key_for_stream_item(si)
    yield (STREAM_ITEMS_TIME_INDEX, ((key[1], key[0]), ''))


# functions take a StreamItem and yield (table name, (key, value))
INDEX_FUNCTIONS = {
    WITH_SOURCE: index_source,
    BY_TIME: index_time,
}


class TableBuffer(object):
    def __init__(self, kvl, table_name, buffer_count=100):
        self.kvl = kvl
        self.table_name = table_name
        self.buffer_count = buffer_count
        self.buffer = []

    def put(self, key, value):
        self.buffer.append((key, value))
        if len(self.buffer) >= self.buffer_count:
            self.kvl.put(self.table_name, *self.buffer)
            self.buffer = []

    def flush(self):
        self.kvl.put(self.table_name, *self.buffer)
        self.buffer = []
