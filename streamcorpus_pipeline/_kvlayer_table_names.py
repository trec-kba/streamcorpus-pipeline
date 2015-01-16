'''table names used by `{from,to}_kvlayer` for `streamcorpus_pipeline`

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2014 Diffeo, Inc.
'''
import base64
import string

# table names:

# kvlayer table name
STREAM_ITEMS_TABLE = 'si'
# key: stream item primary key; value: StreamItem.source. presence means StreamItem.source is not null.
STREAM_ITEMS_SOURCE_INDEX = 'sisi'
# this pair of tables is the keyword index
HASH_TF_INDEX_TABLE = 'sitf'
HASH_KEYWORD_INDEX_TABLE = 'sihk'

STREAM_ITEM_TABLE_DEFS = {
    # StreamItem primary key is:
    # (binay 16 byte md5 of abs_url, timestamp)
    STREAM_ITEMS_TABLE: (str, int),
    # same stream item primary key
    STREAM_ITEMS_SOURCE_INDEX: (str, int),
    # keyword index tables:
    HASH_TF_INDEX_TABLE: (str,),
    HASH_KEYWORD_INDEX_TABLE: (int, str),
    # TODO: maybe reintroduce index-by-time
}

# index names, used for configuration, not kvlayer table names
HASH_TF_SID = 'hash_tf_sid'
HASH_KEYWORD = 'hash_keyword'
WITH_SOURCE = 'with_source'

INDEX_TABLE_NAMES = {
    WITH_SOURCE: STREAM_ITEMS_SOURCE_INDEX,
    HASH_KEYWORD: HASH_KEYWORD_INDEX_TABLE,
    HASH_TF_SID: HASH_TF_INDEX_TABLE,
}


def stream_id_to_kvlayer_key(stream_id):
    '''Convert a text stream ID to a kvlayer key.'''
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
    '''Convert a kvlayer key to a text stream ID.'''
    abs_url_hash, epoch_ticks = k
    return '{}-{}'.format(epoch_ticks, base64.b16encode(abs_url_hash).lower())

