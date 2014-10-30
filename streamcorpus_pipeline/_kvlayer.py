'''Read and write stream items to/from :mod:`kvlayer`.

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2014 Diffeo, Inc.
'''
from __future__ import absolute_import
import ctypes
from collections import Counter
import logging
import string
import uuid


import kvlayer
import streamcorpus
from streamcorpus_pipeline.stages import Configured
from streamcorpus_pipeline._kvlayer_keyword_search import \
    INDEXING_DEPENDENCIES_FAILED, keyword_indexer
from streamcorpus_pipeline._kvlayer_table_names import \
    table_name, all_table_sizes, \
    WITH_SOURCE, DOC_ID_EPOCH_TICKS, \
    HASH_TF_SID, HASH_KEYWORD, \
    epoch_ticks_to_uuid, uuid_to_epoch_ticks, \
    stream_id_to_kvlayer_key, kvlayer_key_to_stream_id


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
        self.client.setup_namespace(
            dict(stream_items=2))

    ## TODO: make this able to take a keyword query as input for
    ## processing stream_ids that match a keyword query; will use the
    ## HASH_TF_SID index created by to_kvlayer;
    ## probably also needs a table about the state of completion of
    ## each StreamItem
    def __call__(self, i_str):
        if i_str:
            # try to parse '{ticks:d}-{doc_id:x}'
            try:
                k = stream_id_to_kvlayer_key(i_str)
                logger.debug('k %r', k)
                key, data = iter_one_or_ex(self.client.get('stream_items', k))
                logger.debug('data! len=%s', len(data))
                errors, data = streamcorpus.decrypt_and_uncompress(data)
                if errors:
                    raise errors
                yield streamcorpus.deserialize(data)
                return
            except WrongNumberException:
                logger.debug('wanted exactly one result', exc_info=True)
                # fall through to try i_str parsed the other way
            except KeyError:
                logger.debug('not a stream-id %r', i_str)
                # fall through to try i_str parsed the other way

            # parse ticks,docid,ticks,docid range key pair
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

        keyword_indexer.check_config(config, name)


    ## TODO:
    # replace existing index keys with non-UUID keys -- cleanup
    # make the stream_ids look more like the bit-packed IDs in keyword index -- optimization
    # add a detected language-type index for prioritizing taggers by language

    index_sizes = all_table_sizes

    def __init__(self, *args, **kwargs):
        super(to_kvlayer, self).__init__(*args, **kwargs)
        self.client = kvlayer.client()
        tables = {table_name(): 2}
        for ndx in self.config['indexes']:
            tables[table_name(ndx)] = self.index_sizes[ndx]
        self.client.setup_namespace(tables)

        if HASH_KEYWORD in self.config['indexes']:
            self.keyword_indexer = keyword_indexer(self.client)

    def __call__(self, t_path, name_info, i_str):
        indexes = {}
        for ndx in self.config['indexes']:
            indexes[ndx] = []

        # The idea here is that the actual documents can be pretty big,
        # so we don't want to keep them in memory, but the indexes
        # will just be UUID tuples.  So the iterator produces...

        def keys_and_values():
            for si in streamcorpus.Chunk(t_path):
                key_key, data = streamitem_to_key_data(si)
                yield key_key, data

                key1, key2 = key_key

                for ndx in indexes:
                    if ndx == DOC_ID_EPOCH_TICKS:
                        kvp = ((key2, key1), r'')
                    elif ndx == WITH_SOURCE:
                        # si.source can be None but we can't write
                        # None blobs to kvlayer
                        if si.source:
                            kvp = ((key1, key2), si.source)
                        else:
                            continue
                    elif ndx == HASH_TF_SID:
                        self.keyword_indexer.index(si)
                        continue
                    elif ndx == HASH_KEYWORD:
                        ## done above
                        continue
                    else:
                        assert False, ('invalid index type ' + ndx)
                    indexes[ndx].append(kvp)

        si_kvps = list(keys_and_values())
        self.client.put(table_name(), *si_kvps)
        for ndx, kvps in indexes.iteritems():
            self.client.put(table_name(ndx), *kvps)

        return [kvlayer_key_to_stream_id(k) for k, v in si_kvps]


def streamitem_to_key_data(si):
    '''
    extract the parts of a StreamItem that go into a kvlayer key, convert StreamItem to blob for storage.

    return (kvlayer key tuple), data blob
    '''
    key1 = epoch_ticks_to_uuid(si.stream_time.epoch_ticks)
    key2 = uuid.UUID(hex=si.doc_id)
    data = streamcorpus.serialize(si)
    errors, data = streamcorpus.compress_and_encrypt(data)
    assert not errors, errors

    return (key1, key2), data
