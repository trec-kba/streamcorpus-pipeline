'''table names used by `{from,to}_kvlayer` for `streamcorpus_pipeline`

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2015 Diffeo, Inc.

This module defines :mod:`kvlayer` table names.  Other modules that
need to access these tables should call:

.. code-block:: python

   import kvlayer
   from streamcorpus_pipeline._kvlayer_table_names import \
     STREAM_ITEM_TABLE_DEFS, STREAM_ITEM_VALUE_DEFS

   client = kvlayer.client()
   client.setup_namespace(STREAM_ITEM_TABLE_DEFS, STREAM_ITEM_VALUE_DEFS)

.. autofunction:: stream_id_to_kvlayer_key
.. autofunction:: kvlayer_key_to_stream_id
.. autofunction:: key_for_stream_item

.. autodata:: STREAM_ITEM_TABLE_DEFS
.. autodata:: STREAM_ITEM_VALUE_DEFS
.. autodata:: STREAM_ITEMS_TABLE
.. autodata:: STREAM_ITEMS_SOURCE_INDEX
.. autodata:: STREAM_ITEMS_TIME_INDEX
.. autodata:: HASH_TF_INDEX_TABLE
.. autodata:: HASH_FREQUENCY_TABLE
.. autodata:: HASH_KEYWORD_INDEX_TABLE
.. autodata:: INDEX_TABLE_NAMES
.. autodata:: WITH_SOURCE
.. autodata:: BY_TIME
.. autodata:: KEYWORDS
.. autodata:: HASH_TF_SID
.. autodata:: HASH_FREQUENCY
.. autodata:: HASH_KEYWORD

'''
from __future__ import absolute_import
import base64
import hashlib
import string

from kvlayer import COUNTER

# table names:

#: Name of the table holding stream items.  Keys are pairs of a
#: 16-byte binary string holding the document ID and an :class:`int`
#: holding the stream time.  Values are compressed serialized stream
#: items.
STREAM_ITEMS_TABLE = 'si'

#: Name of the table holding stream item source strings.  Keys are the
#: same as :data:`STREAM_ITEMS_TABLE`.  Values are simple strings
#: holding the stream item "source" string.  If the record is absent
#: for a given stream item, its source is :const:`None`.
STREAM_ITEMS_SOURCE_INDEX = 'sisi'

#: Name of the table holding stream item keys ordered by time.
#: Keys are the reverse of :data:`STREAM_ITEMS_TABLE`, pairs of
#: an :class:`int` holding the stream time and a 16-byte binary
#: string holding the document ID.  Values are empty.
STREAM_ITEMS_TIME_INDEX = 'siti'

#: Name of the table holding keyword index data.  Keys are tuples of
#: the Murmur hash of the keyword, the document ID, and the stream time;
#: the last two parts make up the stream ID.  Values are ints with the
#: number of times this keyword appears in the document.
HASH_TF_INDEX_TABLE = 'sitf'

#: Name of the table holding document frequency data for hash tokens.
#: Keys are the Murmur hash of the keyword.  Values are ints with the
#: number of documents this hash appears in.
HASH_FREQUENCY_TABLE = 'sihf'

#: Name of the table holding keyword index terms.  Keys are pairs of an
#: integer hash and the corresponding string.  Values are counters
#: holding the number of documents the terms appear in.
HASH_KEYWORD_INDEX_TABLE = 'sihk'

#: Dictionary mapping :mod:`kvlayer` table name to key tuple type,
#: suitable to be passed to
#: :meth:`kvlayer._abstract_storage.AbstractStorage.setup_namespace`
STREAM_ITEM_TABLE_DEFS = {
    # StreamItem primary key is:
    # (binary 16 byte md5 of abs_url, timestamp)
    STREAM_ITEMS_TABLE: (str, int),
    STREAM_ITEMS_SOURCE_INDEX: (str, int),
    STREAM_ITEMS_TIME_INDEX: (int, str),

    # keyword index tables:
    HASH_TF_INDEX_TABLE: (int, str, int),
    HASH_FREQUENCY_TABLE: (int,),
    HASH_KEYWORD_INDEX_TABLE: (int, str),

    # TODO: maybe reintroduce index-by-time
}

#: Dictionary mapping :mod:`kvlayer` table name to value type, suitable
#: to be passed to
#: :meth:`kvlayer._abstract_storage.AbstractStorage.setup_namespace`
STREAM_ITEM_VALUE_DEFS = {
    # Unspecified tables default to str value
    HASH_TF_INDEX_TABLE: int,
    HASH_FREQUENCY_TABLE: COUNTER,
    HASH_KEYWORD_INDEX_TABLE: COUNTER,
}

# index names, used for configuration, not kvlayer table names

#: :class:`~streamcorpus_pipeline._kvlayer.to_kvlayer` index
#: configuration to generate the all of the keyword index tables.
KEYWORDS = 'keywords'

#: :class:`~streamcorpus_pipeline._kvlayer.to_kvlayer` index
#: configuration to generate the document keyword index.
HASH_TF_SID = 'hash_tf_sid'

#: :class:`~streamcorpus_pipeline._kvlayer.to_kvlayer` index
#: configuration to record hash frequencies.
HASH_FREQUENCY = 'hash_frequency'

#: :class:`~streamcorpus_pipeline._kvlayer.to_kvlayer` index
#: configuration to generate the hash-to-keyword index.
HASH_KEYWORD = 'hash_keyword'

#: :class:`~streamcorpus_pipeline._kvlayer.to_kvlayer` index
#: configuration to generate the source-type index.
WITH_SOURCE = 'with_source'

#: :class:`~streamcorpus_pipeline._kvlayer.to_kvlayer` index
#: configuration to generate the timestamp index.
BY_TIME = 'by_time'

#: Dictionary mapping a configuration name from the
#: :class:`~streamcorpus_pipeline._kvlayer.to_kvlayer` writer to
#: its underlying table name.
INDEX_TABLE_NAMES = {
    WITH_SOURCE: STREAM_ITEMS_SOURCE_INDEX,
    BY_TIME: STREAM_ITEMS_TIME_INDEX,
    HASH_FREQUENCY: HASH_FREQUENCY_TABLE,
    HASH_KEYWORD: HASH_KEYWORD_INDEX_TABLE,
    HASH_TF_SID: HASH_TF_INDEX_TABLE,
}


def stream_id_to_kvlayer_key(stream_id):
    '''Convert a text stream ID to a kvlayer key.

    The return tuple can be used directly as a key in the
    :data:`STREAM_ITEMS_TABLE` table.

    :param str stream_id: stream ID to convert
    :return: :mod:`kvlayer` key tuple
    :raise exceptions.KeyError: if `stream_id` is malformed

    '''
    # Reminder: stream_id is 1234567890-123456789abcdef...0
    # where the first part is the (decimal) epoch_ticks and the second
    # part is the (hex) doc_id
    parts = stream_id.split('-')
    if len(parts) != 2:
        raise KeyError('invalid stream_id ' + stream_id)
    epoch_ticks_s = parts[0]
    doc_id_s = parts[1]
    if not epoch_ticks_s.isdigit():
        raise KeyError('invalid stream_id ' + stream_id)
    if doc_id_s.lstrip(string.hexdigits) != '':
        raise KeyError('invalid stream_id ' + stream_id)
    return (base64.b16decode(doc_id_s.upper()), int(epoch_ticks_s))


def kvlayer_key_to_stream_id(k):
    '''Convert a kvlayer key to a text stream ID.

    `k` should be of the same form produced by
    :func:`stream_id_to_kvlayer_key`.

    :param k: :mod:`kvlayer` key tuple
    :return: converted stream ID
    :returntype str:

    '''
    abs_url_hash, epoch_ticks = k
    return '{0}-{1}'.format(epoch_ticks,
                            base64.b16encode(abs_url_hash).lower())


def key_for_stream_item(si):
    '''Get a kvlayer key from a stream item.

    The return tuple can be used directly as a key in the
    :data:`STREAM_ITEMS_TABLE` table.  Note that this recalculates the
    stream ID, and if the internal data on the stream item is inconsistent
    then this could return a different result from
    :func:`stream_id_to_kvlayer_key`.

    :param si: stream item to get key for
    :return: :mod:`kvlayer` key tuple

    '''
    # get binary 16 byte digest
    urlhash = hashlib.md5(si.abs_url).digest()
    return (urlhash, int(si.stream_time.epoch_ticks))
