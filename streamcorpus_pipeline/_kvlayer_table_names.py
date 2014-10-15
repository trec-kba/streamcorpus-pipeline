'''table names used by `{from,to}_kvlayer` for `streamcorpus_pipeline`

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2014 Diffeo, Inc.
'''
import uuid
import ctypes
import string

STREAM_ITEMS_TABLE = 'stream_items'
HASH_TF_SID = 'hash_tf_sid'
HASH_KEYWORD = 'hash_keyword'
DOC_ID_EPOCH_TICKS = 'doc_id_epoch_ticks'
WITH_SOURCE = 'with_source'

def table_name(suffix=None):
    if suffix is None or suffix == STREAM_ITEMS_TABLE:
        return STREAM_ITEMS_TABLE
    return STREAM_ITEMS_TABLE + '_' + suffix

all_table_sizes = {
    STREAM_ITEMS_TABLE: 2,
    DOC_ID_EPOCH_TICKS: 2,
    WITH_SOURCE: 2,
    HASH_TF_SID: (str,),
    HASH_KEYWORD: (int, str),
}

def all_tables():
    return {table_name(name): spec for name, spec in all_table_sizes.items()}


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
    epoch_ticks = epoch_ticks_to_uuid(epoch_ticks_s)
    doc_id = uuid.UUID(hex=doc_id_s)
    return (epoch_ticks, doc_id)


def kvlayer_key_to_stream_id(k):
    '''Convert a kvlayer key to a text stream ID.'''
    (epoch_ticks, doc_id) = k
    epoch_ticks_s = str(uuid_to_epoch_ticks(epoch_ticks))
    doc_id_s = doc_id.hex
    return epoch_ticks_s + '-' + doc_id_s

