'''Read and write stream items to/from :mod:`kvlayer`.

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2014 Diffeo, Inc.
'''
from __future__ import absolute_import
import ctypes
from collections import Counter
import logging
import string
import struct
import uuid

try:
    from backports import lzma
    from sklearn.feature_extraction.text import CountVectorizer
    from many_stop_words import get_stop_words
    import mmh3
    indexing_dependencies_failed = None
except ImportError, exc:
    indexing_dependencies_failed = str(exc)

import kvlayer
import streamcorpus
from streamcorpus_pipeline.stages import Configured
import yakonfig

logger = logging.getLogger(__name__)

STREAM_ITEMS_TABLE = 'stream_items'
HASH_TF_DOC_ID_EPOCH_TICKS = 'hash_tf_doc_id_epoch_ticks'
HASH_KEYWORD   = 'hash_keywords'

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

    ## TODO: make this able to take a keyword query as input for
    ## processing stream_ids that match a keyword query; will use the
    ## HASH_TF_DOC_ID_EPOCH_TICKS index created by to_kvlayer;
    ## probably also needs a table about the state of completion of
    ## each StreamItem
    def __call__(self, i_str):
        if i_str:
            (epoch_ticks_1, doc_id_1, epoch_ticks_2,
             doc_id_2) = i_str.split(',')
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

        for key, data in self.client.scan('stream_items', *key_ranges):
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
    key = stream_id_to_kvlayer_key(stream_id)
    for k, v in client.get('stream_items', key):
        if v is not None:
            errors, bytestr = streamcorpus.decrypt_and_uncompress(v)
            return streamcorpus.deserialize(bytestr)
    raise KeyError(stream_id)


class to_kvlayer(Configured):
    '''Writer that puts stream items in :mod:`kvlayer`.

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
    default_config = {'indexes': ['doc_id_epoch_ticks']}

    @staticmethod
    def check_config(config, name):
        yakonfig.check_toplevel_config(kvlayer, name)
        for ndx in config['indexes']:
            if ndx not in to_kvlayer.index_sizes:
                raise yakonfig.ConfigurationError(
                    'invalid {} indexes type {!r}'
                    .format(name, ndx))

    ## TODO:
    # replace existing index keys with non-UUID keys -- cleanup
    # make the stream_ids look more like the bit-packed IDs in keyword index -- optimization
    # add a detected language-type index for prioritizing taggers by language

    index_sizes = {
        'doc_id_epoch_ticks': 2,
        'with_source': 2,
        ## keyword index on StreamItems
        HASH_TF_DOC_ID_EPOCH_TICKS: (str,),
        ## revere mapping of murmur3 hashes
        HASH_KEYWORD: (int, str),
    }

    def __init__(self, *args, **kwargs):
        super(to_kvlayer, self).__init__(*args, **kwargs)
        self.validate_config()
        self.client = kvlayer.client()
        tables = {STREAM_ITEMS_TABLE: 2}
        for ndx in self.config['indexes']:
            tables[STREAM_ITEMS_TABLE + '_' + ndx] = self.index_sizes[ndx]
        self.client.setup_namespace(tables)

        if HASH_KEYWORD in self.config['indexes']:
            self.analyzer = build_analyzer()

    def validate_config(self):
        if HASH_TF_DOC_ID_EPOCH_TICKS in self.config['indexes'] and \
           HASH_KEYWORD not in self.config['indexes']:
                raise Exception('{} set but {} is not; need both or neither'
                                .format(HASH_TF_DOC_ID_EPOCH_TICKS, HASH_KEYWORD))
        if HASH_KEYWORD in self.config['indexes']:
            if indexing_dependencies_failed:
                raise Exception('indexing is not enabled: {}'
                                .format(indexing_dependencies_failed))
            if HASH_TF_DOC_ID_EPOCH_TICKS not in self.config['indexes']:
                raise Exception('{} set but {} is not; need both or neither'
                                .format(HASH_KEYWORD, HASH_TF_DOC_ID_EPOCH_TICKS))

    def __call__(self, t_path, name_info, i_str):
        indexes = {}
        for ndx in self.config['indexes']:
            indexes[ndx] = []

        # The idea here is that the actual documents can be pretty big,
        # so we don't want to keep them in memory, but the indexes
        # will just be UUID tuples.  So the iterator produces...

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
                        # si.source can be None but we can't write
                        # None blobs to kvlayer
                        if si.source:
                            kvp = ((key1, key2), si.source)
                        else:
                            continue
                    elif ndx == HASH_TF_DOC_ID_EPOCH_TICKS:
                        ## create both "word barrels" style keyword
                        ## reverse index, and also a mapping from
                        ## murmur3 hash value to the input strings.
                        hash_to_word = dict()
                        self.client.put(STREAM_ITEMS_TABLE + '_' + HASH_TF_DOC_ID_EPOCH_TICKS,
                                        *list(keywords(si, self.analyzer, hash_to_word)))
                        self.client.put(STREAM_ITEMS_TABLE + '_' + HASH_KEYWORD,
                                        *hash_to_word.items())
                        continue
                    elif ndx == HASH_KEYWORD:
                        ## done above
                        continue
                    else:
                        assert False, ('invalid index type ' + ndx)
                    indexes[ndx].append(kvp)

        si_kvps = list(keys_and_values())
        self.client.put(STREAM_ITEMS_TABLE, *si_kvps)
        for ndx, kvps in indexes.iteritems():
            self.client.put(STREAM_ITEMS_TABLE + '_' + ndx, *kvps)

        return [kvlayer_key_to_stream_id(k) for k, v in si_kvps]


## used in manipulating 128-bit doc_id hashes into two 64-bit Q ints
max_int64 = 0xFFFFFFFFFFFFFFFF
min_int64 = 0x0000000000000000

### This defines the keyword index keys
## big-endian preserves sorting order
## i = 4-byte signed int
## B = unsigned byte int (character)
## Q = unsigned 8-byte int
### see below for what goes in these fields
reverse_index_packing = '>iBQi'

def keywords(si, analyzer, hash_to_word):
    '''yields packed keys for kvlayer that form a keyword index on
    StreamItems. This uses murmur3 hash of the individual tokens
    generated by sklearn's `CountVectorizer` tokenization and
    filtering based on `many_stop_words`

    Also constructs the hash-->word mapping that inverts the murmur3
    hash, which gets stored as a separate table.

    '''
    if not si.stream_id:
        raise Exception('si.stream_id missing: %r' % si)
    if not si.body:
        logger.warn('no so.body on %s' % si.stream_id)
        return
    if not si.body.clean_visible:
        logger.warn('no so.body.clean_visible on %s' % si.stream_id)
        return
    if not si.stream_time.epoch_ticks:
        raise Exception('si.stream_time.epoch_ticks missing: %s' % si.stream_id)
    if not si.doc_id and len(si.doc_id) == 32:
        raise Exception('si.doc_id does not look like an md5 hash: %r' % si.doc_id)
    epoch_ticks = int(si.stream_time.epoch_ticks)
    doc_id = int(si.doc_id, 16)
    ## split doc_id into two 8-byte things, we only use the first below
    doc_id_1, doc_id_2 = (doc_id >> 64) & max_int64, doc_id & max_int64
    for tok, count in Counter(analyzer(si.body.clean_visible)).items():
        tok_encoded = tok.encode('utf8')
        tok_hash = mmh3.hash(tok_encoded)
        ## put both tok_hash and tok in the key, so we get all
        ## meanings of a hash even if collisions occur
        hash_to_word[(tok_hash, tok_encoded)] = r''
        ## force count to fit within a single byte
        count = max(count, 2**8 - 1)
        ## can drop doc_id_2 and still have 10**19 document space to
        ## avoid collisions; epoch_ticks last allows caller to select
        ## only the last document.
        ### this yields kvlayer-style tuple(key-tuple, value) with all
        ### the info in the key:
        yield (struct.pack(reverse_index_packing, tok_hash, count, doc_id_1, epoch_ticks),), r''


def build_analyzer():
    '''builds an sklearn `CountVectorizer` using `many_stop_words`

    '''
    ## sensible defaults; could need configuration eventually
    cv = CountVectorizer(
        stop_words=get_stop_words(),
        strip_accents='unicode',
    )
    return cv.build_analyzer()

def invert_hash(tok_hash, kvl):
    '''returns a list of all the Unicode strings that have been mapped to
the `tok_hash` integer

    '''
    return [tok_encoded.decode('utf8') 
            for (_, tok_encoded) in 
            kvl.scan(STREAM_ITEMS_TABLE + '_' + HASH_KEYWORD, ((tok_hash,), (tok_hash + 1,)))]

def search(query_string):
    '''queries the keyword index for `word` and yields
    :class:`~streamcorpus.StreamItem` objects that match sorted in
    order of number of times the document references the word.

    warning: this is a very rudimentary search capability

    An analyzer generated by `build_analyzer` is used to tokenize and
    filter the `query_string`.  This can remove tokens.  Results for
    each token are yielded separately, which is equivalent to
    combining the separate tokens in a Boolean OR query.

    '''
    client = kvlayer.client()
    ## this is also done in __init__ above, and could be abstracted out
    tables = {STREAM_ITEMS_TABLE: 2}
    for ndx in to_kvlayer.index_sizes.keys():
        tables[STREAM_ITEMS_TABLE + '_' + ndx] = to_kvlayer.index_sizes[ndx]
    client.setup_namespace(tables)

    analyzer = build_analyzer()
    for tok in analyzer(query_string):
        #import pdb; pdb.set_trace()
        ## build a range query around this token's hash
        tok_hash = mmh3.hash(tok.encode('utf8'))
        tok_start = struct.pack(reverse_index_packing, tok_hash    , 0, 0, 0)
        tok_end   = struct.pack(reverse_index_packing, tok_hash + 1, 0, 0, 0)
        logger.info('%r --> %s' % (tok_hash, invert_hash(tok_hash, client)))
        ## query the keyword index
        for (key,), _ in client.scan(STREAM_ITEMS_TABLE + '_' + HASH_TF_DOC_ID_EPOCH_TICKS,
                               ((tok_start,), (tok_end,))):
            _tok_hash, count, doc_id_1, epoch_ticks = struct.unpack(reverse_index_packing, key)
            ## assert that logic around big-endianness preserving sort
            ## order worked:
            assert tok_start <= key <= tok_end
            assert tok_hash <= _tok_hash <= tok_hash + 1
            ## build a range query around this epoch_ticks-half_doc_id
            doc_id_max_int = (doc_id_1 << 64) | max_int64
            doc_id_min_int = (doc_id_1 << 64) | min_int64
            key1 = epoch_ticks_to_uuid(epoch_ticks)
            key2_max = uuid.UUID(int=doc_id_max_int)
            key2_min = uuid.UUID(int=doc_id_min_int)
            for _, xz_blob in client.scan(STREAM_ITEMS_TABLE, ((key1, key2_min), (key1, key2_max))):
                ## given the half-doc_id this could yield some false positives
                thrift_blob = lzma.decompress(xz_blob)
                yield streamcorpus.deserialize(thrift_blob)


def main():
    import streamcorpus_pipeline
    import argparse
    parser = argparse.ArgumentParser(description='accepts a query string as input and print stream_ids of documents that match *any* of the terms')
    parser.add_argument('query_string', nargs='+', help='enter one or more query words; will be combined with OR')

    modules = [yakonfig, kvlayer]
    args = yakonfig.parse_args(parser, modules)
    config = yakonfig.get_global_config()

    for si in search(' '.join(args.query_string)):
        print si.stream_id
